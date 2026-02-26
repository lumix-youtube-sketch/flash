/**
 * FLASH NEWS BOT v73.0
 * Новое 1: Онбординг — пошаговый выбор категорий при первом /start.
 * Новое 2: Кэш медиа в SQLite (file_id не теряется при перезапуске).
 * Новое 3: Таргетированная рассылка — /broadcast с выбором категорий аудитории.
 * Новое 4: История источников (последние 5) для лучшей диверсификации ленты.
 */

require('dotenv').config();
const { Telegraf, Markup } = require('telegraf');
const RSSParser = require('rss-parser');
const axios = require('axios');
const cheerio = require('cheerio');
const Database = require('better-sqlite3');
const cron = require('node-cron');
const pino = require('pino');
const crypto = require('crypto');
const iconv = require('iconv-lite');
const { createTelegramDispatcher } = require('./src/infrastructure/telegramRateLimiter');
const { setupGracefulShutdown } = require('./src/runtime/gracefulShutdown');

const ADMIN_ID = process.env.ADMIN_ID ? String(process.env.ADMIN_ID).trim() : null;
const logger = pino({ transport: { target: 'pino-pretty', options: { colorize: true } } });

const CONFIG = {
    DB_PATH: 'flash_news_prod.db',
    RSS_TIMEOUT: 15000,
    SCRAPE_TIMEOUT: 10000,
    CONCURRENCY: 25,
    NEWS_TTL_DAYS: 3,
    RETRIES: 1,
    USER_AGENT: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    CIRCUIT_BREAKER_THRESHOLD: 8,
    CIRCUIT_BREAKER_COOLDOWN: 5 * 60 * 1000,
    MAILER_BATCH_SIZE: 800,
    MAX_USER_CACHE: 20000,
    MAX_EPHEMERAL_STATE: 10000
};

const RSSHUB_INSTANCES = [
    'https://rsshub.rssforever.com',
    'https://rsshub.app',
    'https://rss.shab.fun',
    'https://hub.slarker.me',
    'https://rsshub.moeyy.xyz',
    'https://rsshub.mxd.kro.kr',
    'https://rss.itggg.cn'
];
let rsshubIndex = 0;

// Media cache теперь в SQLite — не теряется при перезапуске
const _mediaCacheMap = new Map(); // L1: в памяти для скорости
const MediaCache = {
    get(url) {
        if (_mediaCacheMap.has(url)) return _mediaCacheMap.get(url);
        const row = db.prepare('SELECT file_id FROM media_cache WHERE url=?').get(url);
        if (row) { _mediaCacheMap.set(url, row.file_id); return row.file_id; }
        return null;
    },
    set(url, fileId) {
        _mediaCacheMap.set(url, fileId);
        db.prepare('INSERT OR REPLACE INTO media_cache (url, file_id, saved_at) VALUES (?,?,?)').run(url, fileId, Date.now());
    },
    clear() {
        _mediaCacheMap.clear();
        db.prepare('DELETE FROM media_cache').run();
    }
};

const circuitBreaker = new Map();
function cbKey(url) { try { return new URL(url).hostname; } catch { return url; } }
function isCircuitOpen(urlOrBase) {
    const key = cbKey(urlOrBase);
    const state = circuitBreaker.get(key);
    if (!state) return false;
    if (state.openedAt && Date.now() - state.openedAt < CONFIG.CIRCUIT_BREAKER_COOLDOWN) return true;
    if (state.openedAt) circuitBreaker.set(key, { failures: 0, openedAt: null });
    return false;
}
function recordFailure(urlOrBase) {
    const key = cbKey(urlOrBase);
    const state = circuitBreaker.get(key) || { failures: 0, openedAt: null };
    state.failures += 1;
    if (state.failures >= CONFIG.CIRCUIT_BREAKER_THRESHOLD && !state.openedAt) {
        state.openedAt = Date.now();
        logger.warn(`⚡ Circuit OPEN for host [${key}]`);
    }
    circuitBreaker.set(key, state);
}
function recordSuccess(urlOrBase) {
    circuitBreaker.set(cbKey(urlOrBase), { failures: 0, openedAt: null });
}

const REQUEST_HEADERS = {
    'User-Agent': CONFIG.USER_AGENT,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
};

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function retry(fn, retries = CONFIG.RETRIES) {
    try { return await fn(); } catch (e) {
        if (!retries) throw e;
        await sleep(1000);
        return retry(fn, retries - 1);
    }
}

const isRussian = text => {
    if (!text) return false;
    return (text.match(/[а-яё]/gi) || []).length > (text.match(/[a-z]/gi) || []).length;
};

// Фильтр новостей с пометками AFP/AP/Reuters
const WIRE_AGENCIES = /\b(AFP|AP|Reuters|Рейтер)\b/i;

const CATEGORY_KEYWORDS = {
    finance: ['акци', 'криптовалют', 'биткоин', 'банк', 'инфляци', ' цб ', 'рубл', 'доллар', 'евро', 'инвестици', 'бизнес', 'налог', 'экономик', 'биржа', 'токен'],
    tech: ['смартфон', 'apple', 'интернет', 'нейросеть', ' ии ', ' ai ', 'яндекс', 'google', 'технологи', 'гаджет', 'telegram', 'windows', 'iphone', 'пк', 'софт'],
    sport: ['матч', 'футбол', 'хоккей', 'турнир', 'чемпионат', 'спорт', 'олимпиад', 'медаль', 'клуб', 'тренер', 'стадион'],
    games: ['playstation', 'xbox', 'nintendo', 'steam', ' игр', 'релиз', 'геймер', ' gta ', 'rpg', 'шутер', 'консоль'],
    cinema: ['фильм', 'сериал', 'кино', 'актер', 'режиссер', 'netflix', 'премьер', 'прокат', 'оскар']
};

function guessCategory(text, defaultCategory) {
    if (!text) return defaultCategory;
    const lowerText = text.toLowerCase();
    for (const [cat, keywords] of Object.entries(CATEGORY_KEYWORDS)) {
        if (keywords.some(kw => lowerText.includes(kw))) return cat;
    }
    return defaultCategory;
}

const BANNED_PATTERNS = [
    /(подписыв|подпиш|реклама|erid|промокод|ggsel|скидк|купить|заказать|стрим|stream)/i,
    /(@[a-zA-Zа-яА-Я0-9_]+)/,
    /(комбо|тапалк|хомяк|hamster|бонус|розыгрыш|казино|ставки|airdrop|цитата дня|дейлик|profit|профит|hrum|ton station|blum|notcoin|криптоигр)/i,
    /#[a-zA-Zа-яА-Я0-9_]+/,
    /(смотри\s+\w+\s+прямо сейчас|смотрите\s+\w+\s+прямо сейчас|превью\s+матч|мультиподписк)/i,
    // Рекламные паттерны
    /(предзаказ|pre.?order|оформите|оформить заказ|воспользуйтесь|спонсор|партнёрский|партнерский)/i,
    /(начисляет.*баланс|баланс.*начисляет|кэшбэк|cashback|скидка \d+%|\d+% скидк)/i,
    /(реклама\.|рекламный материал|на правах рекламы|18\+|перейти на сайт|узнать подробнее на)/i,
    /(оператор начисляет|подключить тариф|оформить подписку|первый месяц бесплатно)/i,
    /(ИНН\s*\d+|ОГРН\s*\d+|юридическ|оферт[аы]|срок акции|подробн[её]е на https)/i,
];

const BANNED_IMAGES = ['logo', 'avatar', 'default', 'placeholder', 'share', 'icon', 'rt-logo', 'smi2', 'no-image', 'blank', 'promo', 'banner', 'og_image', 'telegram_logo', 'tgme_widget_message_photo', 'min.jpg'];

const escapeHTML = text => text ? text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;') : '';

const decodeEntities = str => {
    if (!str) return '';
    let decoded = str.replace(/&amp;/g, '&');
    decoded = decoded.replace(/&#(\d+);/g, (match, dec) => String.fromCharCode(dec));
    decoded = decoded.replace(/&#x([0-9a-f]+);/gi, (match, hex) => String.fromCharCode(parseInt(hex, 16)));
    decoded = decoded.replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&nbsp;/g, ' ');
    return decoded;
};

function cleanNewsBody(text) {
    if (!text) return '';
    let t = decodeEntities(text);

    t = t.replace(/[\p{Emoji_Presentation}\p{Extended_Pictographic}]/gu, '');
    t = t.replace(/[\u200B-\u200D\uFEFF]/g, '');

    t = t.replace(/<br\s*\/?>/gi, '\n').replace(/<\/p>/gi, '\n\n');
    // Удаляем гиперссылки, оставляя только текст внутри <a>
    t = t.replace(/<a\s[^>]*href=["'][^"']*["'][^>]*>(.*?)<\/a>/gi, '$1');
    t = t.replace(/<[^>]+>/g, ' ');

    t = t.replace(/\[(?:Media|Photo|Video|Voice|Document|Link|Album)\]/gi, '');
    t = t.replace(/\[\.\.\.\]|\(\.\.\.\)/g, '');
    // Убираем "Video is too big" / "Photo is too big"
    t = t.replace(/Video is too big/gi, '');
    t = t.replace(/Photo is too big/gi, '');

    t = t.replace(/(?:[А-Яа-я-]+\.\s*\d+\s*[а-яА-Я]+\.\s*)?(?:INTERFAX\.RU|ИНТЕРФАКС|Интерфакс)\s*[-—]\s*/gi, '');
    t = t.replace(/ПОДРОБНОСТИ ПОСЛЕДУЮТ/gi, '');
    t = t.replace(/https?:\/\/[^\s]+/gi, '');
    t = t.replace(/t\.me\/[^\s]+/gi, '');
    t = t.replace(/vk\.com\/[^\s]+/gi, '');

    const regexJunk = [
        /Опубликовано.*?\d{4}.*?пользователем[a-zA-Z\. ]*/gi,
        /Опубликовано\s*[а-яА-Я]+,\s*\d{2}\/\d{2}\/\d{4}.*?NEWSmuz\.com/gi,
        /Если вы нашли опечатку.*?Ctrl\+Enter/gi,
        /Выделите фрагмент текста и нажмите.*?/gi,
        /Читать далее.*?$/gi
    ];
    regexJunk.forEach(r => t = t.replace(r, ''));

    const stopWords = [
        'подробнее о "новости', 'мировые новости', 'новости партнеров', 'источник:',
        'фото:', 'смотрите также', 'читайте также', 'подробнее на', 'ранее сообщалось',
        'читайте подробнее', 'больше новостей', 'подписывайтесь на', 'подписаться на',
        'наш канал', 'ссылка в комментариях', '📍', '👉', '👇', 'читайте в ',
        'смотри okko', 'смотрите okko', 'смотри матч', 'смотри на'
    ];
    for (let word of stopWords) {
        let idx = t.toLowerCase().indexOf(word.toLowerCase());
        if (idx !== -1) t = t.substring(0, idx);
    }

    t = t.replace(/[ \t]+/g, ' ');
    t = t.replace(/\n\s*\n/g, '\n\n');
    let paragraphs = t.split('\n\n').map(p => p.replace(/\n/g, ' ').trim()).filter(p => p.length > 10);
    return paragraphs.join('\n\n');
}

const db = new Database(CONFIG.DB_PATH);
db.pragma('journal_mode = WAL');

db.exec(`
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    categories TEXT DEFAULT '[]',
    regions TEXT DEFAULT '[]',
    notification_settings TEXT DEFAULT '{"mode": "smart", "interval": 4, "limit": 3, "silent": false, "last_sent": 0, "last_source": "", "source_history": [], "night_mode": true, "smart_history": []}',
    onboarding_done INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hash TEXT UNIQUE,
    title_hash TEXT UNIQUE,
    title TEXT,
    body TEXT,
    image_url TEXT,
    video_url TEXT,
    source_name TEXT,
    link TEXT,
    category TEXT,
    region TEXT,
    published_at INTEGER,
    merged_sources TEXT DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS seen_log (user_id INTEGER, news_id INTEGER, PRIMARY KEY (user_id, news_id));
CREATE TABLE IF NOT EXISTS media_cache (url TEXT PRIMARY KEY, file_id TEXT, saved_at INTEGER);
CREATE INDEX IF NOT EXISTS idx_news_pub ON news(published_at);
CREATE INDEX IF NOT EXISTS idx_news_cat_pub ON news(category, published_at);
CREATE INDEX IF NOT EXISTS idx_news_reg_pub ON news(region, published_at);
CREATE INDEX IF NOT EXISTS idx_seen_log_news ON seen_log(news_id);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at);
`);

try { db.prepare('ALTER TABLE news ADD COLUMN merged_sources TEXT DEFAULT "[]"').run(); } catch(e) {}
try { db.prepare('ALTER TABLE users ADD COLUMN onboarding_done INTEGER DEFAULT 0').run(); } catch(e) {}

const Repo = {
    getUser(id) {
        let u = db.prepare('SELECT * FROM users WHERE id=?').get(id);
        if (!u) {
            db.prepare('INSERT INTO users (id) VALUES (?)').run(id);
            u = db.prepare('SELECT * FROM users WHERE id=?').get(id);
        }
        const parsedSettings = JSON.parse(u.notification_settings || '{}');
        let cats = JSON.parse(u.categories || '[]');
        if (cats.includes('crypto') || cats.includes('finance_old') || cats.includes('music')) {
            cats = cats.filter(c => c !== 'crypto' && c !== 'finance_old' && c !== 'music');
            if (!cats.includes('finance')) cats.push('finance');
            db.prepare('UPDATE users SET categories=? WHERE id=?').run(JSON.stringify(cats), id);
        }
        return {
            ...u, categories: cats, regions: JSON.parse(u.regions || '[]'),
            onboarding_done: u.onboarding_done || 0,
            notification_settings: {
                mode: parsedSettings.mode || (parsedSettings.enabled === false ? 'off' : 'smart'),
                interval: parsedSettings.interval || 4,
                limit: parsedSettings.limit || 3,
                silent: parsedSettings.silent || false,
                last_sent: parsedSettings.last_sent || 0,
                last_source: parsedSettings.last_source || "",
                source_history: parsedSettings.source_history || [],
                night_mode: parsedSettings.night_mode !== undefined ? parsedSettings.night_mode : true,
                smart_history: parsedSettings.smart_history || []
            }
        };
    },
    saveNews(n) {
        if (!n.body || n.body.length < 50) return false;
        const tHash = crypto.createHash('md5').update(n.title.toLowerCase().replace(/[^а-яёa-z0-9]/g, '')).digest('hex');
        try {
            db.prepare(`INSERT INTO news (hash,title_hash,title,body,image_url,video_url,source_name,link,category,region,published_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)`).run(n.hash, tHash, n.title, n.body, n.image, n.video, n.source, n.link, n.cat, n.reg, n.pub);
            return true;
        } catch { return false; }
    },
    getUnseen(uid, cats, regs, limit = 1) {
        if (!cats.length && !regs.length) return [];
        const conditions = []; const args = [];
        if (cats.length) { conditions.push(`category IN (${cats.map(()=>'?').join(',')})`); args.push(...cats); }
        if (regs.length) { conditions.push(`region IN (${regs.map(()=>'?').join(',')})`); args.push(...regs); }
        return db.prepare(`SELECT * FROM news WHERE (${conditions.join(' OR ')}) AND length(body) > 50 AND id NOT IN (SELECT news_id FROM seen_log WHERE user_id=?) ORDER BY published_at DESC LIMIT ?`)
            .all(...args, uid, limit * 10);
    }
};

const userCache = new Map();

function pruneMap(map, maxSize) {
    if (map.size <= maxSize) return;
    const removeCount = Math.floor(maxSize * 0.25);
    let i = 0;
    for (const key of map.keys()) {
        map.delete(key);
        i += 1;
        if (i >= removeCount) break;
    }
}

const CachedRepo = {
    get(id) {
        const cached = userCache.get(id);
        if (cached && Date.now() - cached.ts < 60000) return cached.user;
        const user = Repo.getUser(id);
        userCache.set(id, { user, ts: Date.now() });
        pruneMap(userCache, CONFIG.MAX_USER_CACHE);
        return user;
    },
    saveSettings(user) {
        db.prepare('UPDATE users SET notification_settings=? WHERE id=?').run(JSON.stringify(user.notification_settings), user.id);
        userCache.set(user.id, { user, ts: Date.now() });
    },
    saveCategories(user) {
        db.prepare('UPDATE users SET categories=? WHERE id=?').run(JSON.stringify(user.categories), user.id);
        userCache.set(user.id, { user, ts: Date.now() });
    },
    saveRegions(user) {
        db.prepare('UPDATE users SET regions=? WHERE id=?').run(JSON.stringify(user.regions), user.id);
        userCache.set(user.id, { user, ts: Date.now() });
    },
    saveOnboarding(user) {
        db.prepare('UPDATE users SET onboarding_done=?, categories=? WHERE id=?').run(1, JSON.stringify(user.categories), user.id);
        user.onboarding_done = 1;
        userCache.set(user.id, { user, ts: Date.now() });
    }
};

function findBayan(title, category) {
    if (!category) return null;
    const cutoff = Date.now() - 12 * 60 * 60 * 1000;
    const recentNews = db.prepare('SELECT id, title, source_name, merged_sources FROM news WHERE category = ? AND published_at > ? ORDER BY published_at DESC LIMIT 350').all(category, cutoff);
    const getRoots = str => str.toLowerCase().replace(/[^а-яёa-z0-9]/gi, ' ').split(/\s+/).filter(w => w.length > 4).map(w => w.slice(0, 5));
    const roots1 = new Set(getRoots(title));
    if (roots1.size === 0) return null;
    for (let news of recentNews) {
        const roots2 = new Set(getRoots(news.title));
        if (roots2.size === 0) continue;
        let intersection = 0;
        for (let w of roots1) { if (roots2.has(w)) intersection++; }
        if ((intersection / Math.min(roots1.size, roots2.size)) >= 0.6) return news;
    }
    return null;
}

const TG = (slug, name, opts = {}) => ({ u: `${RSSHUB_INSTANCES[0]}/telegram/channel/${slug}`, n: name, tgSlug: slug, ...opts });

const CATEGORIES = {
    world: { name: '⚡️ Молнии', urls: [
        // Удалены: ТАСС (tass_agency), РИА Новости (rian_ru)
        TG('bazabazon', 'Baza'), TG('mash', 'Mash'),
        TG('topor', 'Топор'), TG('lentach', 'Лентач'),
        TG('shot_shot', 'SHOT'), TG('moscowach', 'Москвач'), TG('smotri_media', 'Смотри'),
        TG('news_ru', 'NEWS.ru'), TG('varlamov_news', 'Varlamov News')
    ]},
    tech: { name: '📱 Техно', urls: [
        { u: 'https://habr.com/ru/rss/news/?fl=ru', n: 'Habr' },
        { u: 'https://www.ixbt.com/export/news.rss', n: 'iXBT' },
        { u: 'https://www.ferra.ru/exports/rss.xml', n: 'Ferra' },
        { u: 'https://3dnews.ru/news/rss/', n: '3DNews' },
        TG('exploitex', 'Эксплойт'), TG('d_code', 'Код Дурова'), TG('rozetked', 'Rozetked'),
        TG('wylsared', 'Wylsacom Red'), TG('droiderru', 'Droider'),
        TG('techsparks', 'TechSparks'), TG('ai_machinelearning_ru', 'AI Новости'),
        TG('mobile_review_com', 'Mobile Review'), TG('trashbox_ru', 'Trashbox'),
        TG('the_code_media', 'Код (Яндекс)'), TG('proglib', 'Библиотека Программиста'),
        TG('sysodmins', 'Сисодмины')
    ]},
    finance: { name: '💰 Финансы & Крипта', urls: [
        { u: 'https://ru.investing.com/rss/news_25.rss', n: 'Investing (Акции)' },
        { u: 'https://bits.media/rss2/', n: 'Bits.media' },
        { u: 'https://forklog.com/feed', n: 'ForkLog' },
        { u: 'https://rssexport.rbc.ru/rbcnews/news/16/full.rss', n: 'РБК Инвестиции' },
        TG('rbc_crypto', 'РБК Крипто'), TG('banksta', 'Банкста'), TG('if_market', 'IF Market'),
        TG('market_twits', 'MarketTwits'), TG('ru_forklog', 'ForkLog TG'), TG('cryptonews_ru', 'CryptoNews'),
        TG('cbrstocks', 'ЦБ и рынки'), TG('bitkogan', 'Bitkogan'), TG('finam_alert', 'Финам TG'),
        TG('tinkoff_invest_official', 'Т-Инвестиции'), TG('alfa_investments', 'Альфа-Инвестиции'),
        TG('sberinvestments', 'СберИнвестиции'), TG('gazprombank_investments', 'ГПБ Инвестиции')
    ]},
    games: { name: '🎮 Игры', urls: [
        // Удалён StopGame RSS — только TG-канал
        { u: 'https://www.igromania.ru/rss/news.xml', n: 'Игромания' },
        { u: 'https://www.playground.ru/rss/news.xml', n: 'PlayGround' },
        { u: 'https://dtf.ru/rss/all', n: 'DTF' },
        TG('ignrussia', 'IGN Russia'), TG('stopgameru', 'StopGame TG'), TG('igromania', 'Игромания TG'),
        TG('vgtimes', 'VGTimes TG'), TG('ixbt_games', 'iXBT Games'), TG('cybersportru', 'Cybersport.ru'),
        TG('xbox_ru', 'Xbox RU'), TG('playstation_ru', 'PlayStation RU'), TG('gabefollower', 'Gabe Follower'),
        TG('cb_games', 'КБ. Игры'), TG('igry7', 'Все про игры'), TG('app2top', 'App2Top'), TG('gamedevjob', 'Gamedev')
    ]},
    sport: { name: '⚽ Спорт', urls: [
        // Удалён Okko Спорт. Добавлен Спорт Mail.ru (с фильтром AFP/AP/Reuters в ingester)
        { u: 'https://sport.mail.ru/rss/news/', n: 'Спорт Mail.ru' },
        TG('matchtv', 'Матч ТВ'), TG('sportsru', 'Sports.ru TG'), TG('sportexpress', 'Спорт-Экспресс TG'),
        TG('fc_zhivoy_futbol', 'Живой Футбол'), TG('matchpremier', 'Матч Премьер'),
        TG('sport24_ru', 'Sport24'), TG('khl_official_telegram', 'КХЛ'), TG('eurosport_ru', 'Eurosport'),
        TG('f1_ru', 'Формула 1'), TG('tennis_ru', 'Теннис'), TG('sportria', 'РИА Спорт')
    ]},
    cinema: { name: '🍿 Кино', urls: [
        { u: 'https://www.kinonews.ru/rss/', n: 'KinoNews' },
        { u: 'https://ovideo.ru/rss', n: 'Ovideo' },
        { u: 'https://www.kino-teatr.ru/rss/news.xml', n: 'Кино-Театр.РУ' },
        { u: 'https://shazoo.ru/news/rss', n: 'Shazoo' },
        TG('kinopoisk', 'Кинопоиск'), TG('kinopoisk_soon', 'Кинопоиск Новости'),
        TG('cinemaholics', 'Cinemaholics'), TG('kino_tv_ru', 'Кино ТВ'),
        TG('kinoart_ru', 'Искусство кино'), TG('serialsru', 'Сериалы RU'), TG('movies_in_details', 'Фильмы в деталях'),
        TG('newsjetflix', 'JETFLIXnews'), TG('kino_v_detalyah', 'Кино в деталях'), TG('kinobug', 'Кинобаг'),
        TG('chucky_store', 'Чак Review'), TG('kinokrad_official', 'Кинокрад'), TG('rhymes_kino', 'Рифмы и Кино'),
        TG('kino_first', 'Кино первого'), TG('kinomania', 'Киномания'), TG('netflix_ru', 'Netflix Ru')
    ]},
};

const REGIONS = {
    moscow: { name: '🏰 Москва', urls: ['https://vm.ru/rss', 'https://www.m24.ru/rss.xml', 'https://dni.ru/rss.xml'] },
    spb: { name: '⚓️ СПб', urls: ['https://spb.aif.ru/rss/all.php', 'https://moika78.ru/feed/'] },
    nsk: { name: '❄️ Нск', urls: ['https://nsk.aif.ru/rss/all.php', 'https://sibkray.ru/rss/', 'https://ndn.info/rss/'] },
    ekb: { name: '⛰ Екб', urls: ['https://ural.aif.ru/rss/all.php'] },
    kzn: { name: '🕌 Казань', urls: ['https://116.ru/rss', 'https://kazan.aif.ru/rss/all.php', 'https://www.tatar-inform.ru/rss'] },
    nn: { name: '🏰 НН', urls: ['https://nn.aif.ru/rss/all.php', 'https://opennov.ru/rss'] },
    krd: { name: '☀️ Краснодар', urls: ['https://93.ru/rss', 'https://kuban.aif.ru/rss/all.php', 'https://kubnews.ru/rss/', 'https://www.yuga.ru/articles.rss'] },
    rostov: { name: '🐎 Ростов', urls: ['https://161.ru/rss', 'https://rostov.aif.ru/rss/all.php', 'https://www.1rnd.ru/rss'] },
    vladik: { name: '🌊 Владивосток', urls: ['https://www.newsvl.ru/rss/', 'https://vl.aif.ru/rss/all.php'] },
    chelyabinsk: { name: '🏭 Челябинск', urls: ['https://74.ru/rss', 'https://chel.aif.ru/rss/all.php', 'https://ura.news/rss'] },
    samara: { name: '🚀 Самара', urls: ['https://63.ru/rss', 'https://samara.aif.ru/rss/all.php', 'https://volga.news/rss/'] },
    ufa: { name: '🍯 Уфа', urls: ['https://ufa1.ru/rss', 'https://ufa.aif.ru/rss/all.php'] },
    krasnoyarsk: { name: '🌲 Красноярск', urls: ['https://krsk.aif.ru/rss/all.php', 'https://www.sibnovosti.ru/rss'] },
    perm: { name: '🐻 Пермь', urls: ['https://59.ru/rss', 'https://perm.aif.ru/rss/all.php'] },
    voronezh: { name: '🏙 Воронеж', urls: ['https://vrn.aif.ru/rss/all.php', 'https://riavrn.ru/rss/', 'https://moe-online.ru/rss'] }
};

// ---------------- INGESTER ----------------
const Ingester = {
    parser: new RSSParser({ timeout: CONFIG.RSS_TIMEOUT, headers: REQUEST_HEADERS }),

    isValidMedia(url) {
        if (!url || typeof url !== 'string') return false;
        if (!url.startsWith('http')) return false;
        const lowerUrl = url.toLowerCase();
        return !BANNED_IMAGES.some(badWord => lowerUrl.includes(badWord));
    },

    async scrape(url, title) {
        try {
            const resp = await retry(() => axios.get(url, {
                timeout: CONFIG.SCRAPE_TIMEOUT, headers: REQUEST_HEADERS, responseType: 'arraybuffer'
            }));
            const contentType = resp.headers['content-type'] || '';
            let data = /windows-1251|cp1251|cp-1251/i.test(contentType) ? iconv.decode(Buffer.from(resp.data), 'win1251') : iconv.decode(Buffer.from(resp.data), 'utf-8');
            const $ = cheerio.load(data);
            let img = $('meta[property="og:image"]').attr('content') || $('meta[name="twitter:image"]').attr('content') || $('article img').first().attr('src');
            if (img && !img.startsWith('http')) { try { img = new URL(img, url).href; } catch(e) { img = null; } }
            let paragraphs = [];
            const selectors = ['article p', '.article__text p', '.content p', '.post-content p', '.topic-body p', 'main p'];
            $(selectors.join(', ')).each((i, el) => {
                const t = $(el).text().trim();
                if (t.length > 50 && t !== title && paragraphs.length < 12) paragraphs.push(t);
            });
            return { img: img, text: paragraphs.join('\n\n') };
        } catch { return { img: null, text: null }; }
    },

    async scrapeTelegramEmbed(url) {
        try {
            const embedUrl = url.includes('?') ? url + '&embed=1&single=1' : url + '?embed=1&single=1';
            const resp = await axios.get(embedUrl, { timeout: 2500, headers: REQUEST_HEADERS });
            const $tg = cheerio.load(resp.data);
            let img = null, vid = null;

            // Видео
            const vSrc = $tg('video.tgme_widget_message_video').attr('src') || $tg('video source').attr('src');
            if (vSrc && vSrc.startsWith('http')) vid = vSrc;

            // Фото из style
            if (!vid) {
                const style = $tg('.tgme_widget_message_photo_image').attr('style');
                if (style) {
                    const m = style.match(/url\(['"]?(.*?)['"]?\)/);
                    if (m && m[1].startsWith('http')) img = m[1];
                }
            }

            // Фото из img тега (запасной вариант)
            if (!img && !vid) {
                $tg('img.tgme_widget_message_photo_image, .tgme_widget_message_photo img').each((_, el) => {
                    const s = $tg(el).attr('src');
                    if (s && s.startsWith('http') && !img) img = s;
                });
            }

            return { img, vid };
        } catch(e) { return { img: null, vid: null }; }
    },

    async fetchSource(src) {
        if (src.tgSlug) {
            let lastErr = null;
            for (let attempt = 0; attempt < RSSHUB_INSTANCES.length; attempt++) {
                const base = RSSHUB_INSTANCES[(rsshubIndex + attempt) % RSSHUB_INSTANCES.length];
                if (isCircuitOpen(`${base}/`)) continue;
                try {
                    const feed = await this.parser.parseURL(`${base}/telegram/channel/${src.tgSlug}`);
                    if (!feed || !feed.items || feed.items.length === 0) throw new Error('Empty feed');
                    recordSuccess(base);
                    rsshubIndex = (rsshubIndex + attempt) % RSSHUB_INSTANCES.length;
                    return feed;
                } catch(e) { recordFailure(base); lastErr = e.message; }
            }
            if (lastErr) logger.warn(`[TG Fail] Could not fetch ${src.tgSlug} from any instance. Last error: ${lastErr}`);
            return null;
        }
        if (isCircuitOpen(src.u)) return null;
        try {
            const feed = await retry(() => this.parser.parseURL(src.u));
            recordSuccess(src.u); return feed;
        } catch(e) { recordFailure(src.u); return null; }
    },

    async run() {
        logger.info('Ingester cycle started');
        let added = 0; const sources = [];
        Object.keys(CATEGORIES).forEach(k => CATEGORIES[k].urls.forEach(u => sources.push({ ...u, cat: k, reg: null })));
        Object.keys(REGIONS).forEach(k => REGIONS[k].urls.forEach(u => sources.push({ u, n: REGIONS[k].name, cat: null, reg: k })));

        for (let i = 0; i < sources.length; i += CONFIG.CONCURRENCY) {
            const chunk = sources.slice(i, i + CONFIG.CONCURRENCY);
            await Promise.all(chunk.map(async src => {
                const feed = await this.fetchSource(src);
                if (!feed) return;

                for (const item of (feed.items || []).slice(0, 40)) {
                    let rawTitle = decodeEntities(item.title || '');
                    rawTitle = rawTitle.replace(/\[(?:Media|Photo|Video|Voice|Document|Link|Album)\]/gi, '').replace(/\[\.\.\.\]|\(\.\.\.\)/g, '');
                    rawTitle = rawTitle.replace(/[\p{Emoji_Presentation}\p{Extended_Pictographic}]/gu, '');
                    rawTitle = rawTitle.trim();

                    const isTgSource = !!src.tgSlug;

                    // Пропускаем пересланные посты
                    if (isTgSource) {
                        const bodyRaw = item['content:encoded'] || item.content || item.description || '';
                        if (/Forwarded from|>Переслано из</i.test(bodyRaw)) continue;
                        if (/^Forwarded from/i.test(rawTitle)) continue;
                        if (item.author && item.author !== src.n && item.author !== src.tgSlug) continue;
                    }

                    // Фильтр AFP/AP/Reuters для RSS-источников
                    if (!isTgSource) {
                        const checkText = rawTitle + ' ' + (item.description || '') + ' ' + (item['content:encoded'] || '');
                        if (WIRE_AGENCIES.test(checkText)) continue;
                    }

                    // Ранняя проверка на рекламу по сырому телу (до cleanBody)
                    const rawBodyForCheck = (item['content:encoded'] || item.content || item.description || '').toLowerCase();
                    const HARD_AD_PATTERNS = [
                        /erid[\s:]/i, /инн\s*\d{10}/i, /огрн\s*\d/i,
                        /на правах рекламы/i, /рекламодатель/i, /срок акции/i,
                        /подробнее на https/i, /реклама\s*\./i, /0+\s*руб.*баланс/i
                    ];
                    if (HARD_AD_PATTERNS.some(rx => rx.test(rawBodyForCheck))) continue;

                    let bodyText = item['content:encoded'] || item.content || item.description || item.contentSnippet || '';
                    let finalVid = null;
                    let finalImg = null;

                    // ШАГ 1: enclosure (самый быстрый — уже в RSS)
                    if (item.enclosure && item.enclosure.url) {
                        if (item.enclosure.type && item.enclosure.type.startsWith('video/') && this.isValidMedia(item.enclosure.url)) {
                            finalVid = item.enclosure.url;
                        } else if (this.isValidMedia(item.enclosure.url)) {
                            finalImg = item.enclosure.url;
                        }
                    }

                    // ШАГ 2: media:content / media:thumbnail (стандартные RSS media-поля)
                    if (!finalImg && !finalVid) {
                        const mc = item['media:content'] || item['media:thumbnail'];
                        if (mc) {
                            const mcUrl = mc.$ ? mc.$.url : (typeof mc === 'string' ? mc : null);
                            if (mcUrl && this.isValidMedia(mcUrl)) finalImg = mcUrl;
                        }
                    }

                    // ШАГ 3: og:image из HTML-тела (быстро — уже в RSS, не делаем запрос)
                    if (!finalImg && bodyText) {
                        const $b = cheerio.load(bodyText);

                        if (isTgSource) {
                            // TG: ищем video и img в теле RSS
                            $b('video, source').each((_, el) => {
                                const s = $b(el).attr('src');
                                if (s && s.startsWith('http') && !finalVid) finalVid = s;
                            });
                            $b('img').each((_, el) => {
                                const s = $b(el).attr('src') || $b(el).attr('data-src');
                                if (s && s.startsWith('http') && !s.includes('emoji') && !s.includes('avatar') && !finalImg) finalImg = s;
                            });
                        } else {
                            // RSS: og:image часто вставляется прямо в description как <img>
                            $b('img').each((_, el) => {
                                const s = $b(el).attr('src');
                                if (s && s.startsWith('http') && this.isValidMedia(s) && !finalImg) finalImg = s;
                            });
                        }
                    }

                    // ШАГ 4: scrape страницы — только для RSS-источников без картинки
                    // Для TG-источников это бессмысленно (t.me требует авторизацию)
                    let scrapedText = null;
                    if (!isTgSource) {
                        const details = await this.scrape(item.link, rawTitle);
                        if (details.text && details.text.length > 50) scrapedText = details.text;
                        if (!finalImg && this.isValidMedia(details.img)) finalImg = details.img;
                    }
                    if (scrapedText) bodyText = scrapedText;

                    // ШАГ 5: TG embed — ТОЛЬКО если после всех шагов картинки нет
                    // Самый медленный шаг, вызываем в последнюю очередь
                    if (isTgSource && !finalImg && !finalVid) {
                        const tgMedia = await this.scrapeTelegramEmbed(item.link);
                        if (tgMedia.vid && this.isValidMedia(tgMedia.vid)) finalVid = tgMedia.vid;
                        else if (tgMedia.img && this.isValidMedia(tgMedia.img)) finalImg = tgMedia.img;
                    }

                    let cleanBody = cleanNewsBody(bodyText);

                    if (isTgSource) {
                        const rxChannel = new RegExp('^' + src.n + '\\s*[:-]?\\s*', 'i');
                        cleanBody = cleanBody.replace(rxChannel, '').trim();

                        let parts = cleanBody.split('\n\n');
                        if (parts.length > 0) {
                            let potentialTitle = parts[0].trim();
                            if (potentialTitle.length > 10 && potentialTitle.length <= 200) {
                                rawTitle = potentialTitle;
                                cleanBody = parts.slice(1).join('\n\n').trim();
                            } else {
                                let firstSentence = potentialTitle.match(/^.*?[.?!](?:\s|$)/);
                                if (firstSentence) {
                                    rawTitle = firstSentence[0].trim();
                                } else {
                                    rawTitle = potentialTitle.slice(0, 180) + '...';
                                }
                            }
                        }

                        let cleanTitleNorm = rawTitle.replace(/[^а-яёa-z0-9]/gi, '').toLowerCase();
                        let bodyStartNorm = cleanBody.slice(0, rawTitle.length + 50).replace(/[^а-яёa-z0-9]/gi, '').toLowerCase();
                        if (cleanTitleNorm.length > 10 && bodyStartNorm.includes(cleanTitleNorm)) {
                            let cutIndex = cleanBody.toLowerCase().indexOf(rawTitle.toLowerCase().slice(0, 15));
                            if (cutIndex !== -1) {
                                cleanBody = cleanBody.slice(cutIndex + rawTitle.length).trim();
                                cleanBody = cleanBody.replace(/^[.,:;\-—|]+\s*/, '');
                            }
                        }

                        cleanBody = cleanBody.replace(/^[.,:;\-—|]+\s*/, '');
                        rawTitle = rawTitle.replace(/^[.,:;\-—|]+\s*/, '');
                        rawTitle = rawTitle.replace(/[\p{Emoji_Presentation}\p{Extended_Pictographic}]/gu, '').replace(/\s+/g, ' ').trim();

                        let tgParagraphs = cleanBody.split('\n\n');
                        if (tgParagraphs.length > 2) cleanBody = tgParagraphs.slice(0, 2).join('\n\n');
                    }

                    if (!item.link || !rawTitle || !isRussian(rawTitle)) continue;
                    if (BANNED_PATTERNS.some(rx => rx.test(rawTitle))) continue;

                    let finalCat = src.cat;
                    if (src.dynamic) finalCat = guessCategory(rawTitle + ' ' + cleanBody, src.cat);

                    const bayan = findBayan(rawTitle, finalCat);
                    if (bayan) {
                        if (bayan.source_name !== src.n) {
                            let merged = JSON.parse(bayan.merged_sources || '[]');
                            if (!merged.includes(src.n)) {
                                merged.push(src.n);
                                db.prepare('UPDATE news SET merged_sources=? WHERE id=?').run(JSON.stringify(merged), bayan.id);
                            }
                        }
                        continue;
                    }

                    if (!cleanBody || cleanBody.length < 50) continue;
                    if (BANNED_PATTERNS.some(rx => rx.test(cleanBody))) continue;

                    if (Repo.saveNews({
                        hash: crypto.createHash('md5').update(item.link).digest('hex'),
                        title: rawTitle, body: cleanBody.slice(0, 1500).trim(),
                        image: finalImg, video: finalVid, isTg: isTgSource,
                        source: src.n, link: item.link, cat: finalCat, reg: src.reg,
                        pub: new Date(item.pubDate || item.isoDate).getTime() || Date.now()
                    })) added++;
                }
            }));
        }
        logger.info(`Ingester cycle finished. Added +${added} items.`);
    }
};

// ---------------- БОТ UI BUILDERS ----------------
const bot = new Telegraf(process.env.BOT_TOKEN);
const telegramDispatcher = createTelegramDispatcher({ logger, maxPerSecond: 28 });

bot.catch((err, ctx) => {
    logger.error(`[Global Error] ${ctx.updateType}: ${err.message}`);
});

const bottomMenu = Markup.keyboard([['📱 Меню']]).resize();

const getMenu = user => {
    const keys = Object.keys(CATEGORIES); const btns = [];
    for (let i = 0; i < keys.length; i += 2) {
        const row = [Markup.button.callback(`${user.categories.includes(keys[i]) ? '✅' : '⬜'} ${CATEGORIES[keys[i]].name}`, `toggle_cat_${keys[i]}`)];
        if (keys[i+1]) row.push(Markup.button.callback(`${user.categories.includes(keys[i+1]) ? '✅' : '⬜'} ${CATEGORIES[keys[i+1]].name}`, `toggle_cat_${keys[i+1]}`));
        btns.push(row);
    }
    btns.push([Markup.button.callback(`📍 Выбрать регионы (${user.regions.length})`, 'menu_reg')]);
    btns.push([Markup.button.callback('⚙️ Настройки рассылки', 'menu_settings')]);
    btns.push([Markup.button.callback('🚀 СЛЕДУЮЩАЯ НОВОСТЬ', 'next'), Markup.button.callback('ℹ️ Инфо', 'show_info')]);
    return Markup.inlineKeyboard(btns);
};

const getRegionsMenu = user => {
    const keys = Object.keys(REGIONS); const btns = [];
    for (let i = 0; i < keys.length; i += 2) {
        const row = [Markup.button.callback(`${user.regions.includes(keys[i]) ? '✅' : '⬜'} ${REGIONS[keys[i]].name}`, `toggle_reg_${keys[i]}`)];
        if (keys[i+1]) row.push(Markup.button.callback(`${user.regions.includes(keys[i+1]) ? '✅' : '⬜'} ${REGIONS[keys[i+1]].name}`, `toggle_reg_${keys[i+1]}`));
        btns.push(row);
    }
    btns.push([Markup.button.callback('⬅️ Назад в меню', 'back_main')]);
    return Markup.inlineKeyboard(btns);
};

function getSettingsText(mode) {
    if (mode === 'smart') return `🧠 <b>Режим: УМНАЯ ЛЕНТА</b>\n\nБот присылает новости органично, по одной.\nОтправляются <b>только главные события</b>.\n<i>Лимит: не более 5 новостей за 2 часа.</i>`;
    if (mode === 'custom') return `⏱ <b>Режим: ПО РАСПИСАНИЮ</b>\n\nБот копит все новости по вашим интересам и присылает их пачкой раз в выбранный интервал.`;
    return `🔴 <b>Режим: ВЫКЛЮЧЕНА</b>\n\nАвтоматической рассылки нет. Вы можете читать новости только вручную по кнопке "Следующая новость".`;
}

const getSettingsMenu = user => {
    const s = user.notification_settings;
    const kb = [[Markup.button.callback(`${s.mode === 'smart' ? '🧠 Умная' : 'Умная'}`, 'set_mode_smart'), Markup.button.callback(`${s.mode === 'custom' ? '⏱ Настр.' : 'Настр.'}`, 'set_mode_custom'), Markup.button.callback(`${s.mode === 'off' ? '🔴 Выкл' : 'Выкл'}`, 'set_mode_off')]];
    if (s.mode === 'custom') kb.push([Markup.button.callback(`Интервал: ${s.interval === 0.5 ? '30 мин' : s.interval + 'ч'}`, 'cycle_interval'), Markup.button.callback(`Лимит: ${s.limit} шт.`, 'cycle_limit')]);
    if (s.mode !== 'off') kb.push([Markup.button.callback(`${s.silent ? '🔇 Без звука' : '🔊 Со звуком'}`, 'toggle_silent'), Markup.button.callback(`🌙 Сон (23-08): ${s.night_mode ? '💤 Вкл' : '🔔 Выкл'}`, 'toggle_night')]);
    kb.push([Markup.button.callback('⬅️ Назад в меню', 'back_main')]);
    return Markup.inlineKeyboard(kb);
};

// ---- ОНБОРДИНГ ----
// Состояние онбординга хранится в памяти (временно, до завершения)
const onboardingState = new Map(); // userId -> { step, selectedCats }

function getOnboardingCatsMenu(selectedCats) {
    const keys = Object.keys(CATEGORIES);
    const btns = [];
    for (let i = 0; i < keys.length; i += 2) {
        const row = [Markup.button.callback(
            `${selectedCats.includes(keys[i]) ? '✅' : '⬜'} ${CATEGORIES[keys[i]].name}`,
            `ob_cat_${keys[i]}`
        )];
        if (keys[i + 1]) row.push(Markup.button.callback(
            `${selectedCats.includes(keys[i + 1]) ? '✅' : '⬜'} ${CATEGORIES[keys[i + 1]].name}`,
            `ob_cat_${keys[i + 1]}`
        ));
        btns.push(row);
    }
    btns.push([Markup.button.callback(
        selectedCats.length > 0 ? `✨ Готово (выбрано: ${selectedCats.length})` : '⬜ Выберите хотя бы одну',
        selectedCats.length > 0 ? 'ob_done' : 'ob_none'
    )]);
    return Markup.inlineKeyboard(btns);
}

bot.start(async ctx => {
    const userId = ctx.from.id;
    const user = CachedRepo.get(userId);

    // Повторный /start у уже настроенного пользователя — просто меню
    if (user.onboarding_done) {
        const usersCount = db.prepare('SELECT COUNT(*) as c FROM users').get().c;
        await ctx.reply('👋 С возвращением!');
        return ctx.reply('Главное меню:', getMenu(user));
    }

    // Новый пользователь — онбординг
    const usersCount = db.prepare('SELECT COUNT(*) as c FROM users').get().c;
    onboardingState.set(userId, { selectedCats: [] });
    pruneMap(onboardingState, CONFIG.MAX_EPHEMERAL_STATE);

    await ctx.reply(
        `⚡️ <b>Добро пожаловать во Flash News!</b>\n\n` +
        `Я агрегирую новости из десятков источников и присылаю только то, что интересно <b>именно тебе</b>.\n\n` +
        `👥 Уже читают: <b>${usersCount}</b> человек\n\n` +
        `Давай настроим твою ленту — это займёт 10 секунд.`,
        { parse_mode: 'HTML', ...bottomMenu }
    );

    await ctx.reply(
        `📌 <b>Шаг 1 из 2 — Выбери темы</b>\n\nОтметь всё, что тебе интересно:`,
        { parse_mode: 'HTML', ...getOnboardingCatsMenu([]) }
    );
});

// Тоггл категории в онбординге
bot.action(/ob_cat_(.+)/, async ctx => {
    ctx.answerCbQuery().catch(() => {});
    const userId = ctx.from.id;
    const cat = ctx.match[1];
    const state = onboardingState.get(userId) || { selectedCats: [] };

    state.selectedCats = state.selectedCats.includes(cat)
        ? state.selectedCats.filter(c => c !== cat)
        : [...state.selectedCats, cat];
    onboardingState.set(userId, state);

    ctx.editMessageReplyMarkup(getOnboardingCatsMenu(state.selectedCats).reply_markup).catch(() => {});
});

// Нажали "Готово" в онбординге
bot.action('ob_done', async ctx => {
    ctx.answerCbQuery().catch(() => {});
    const userId = ctx.from.id;
    const state = onboardingState.get(userId) || { selectedCats: [] };
    const user = CachedRepo.get(userId);
    user.categories = state.selectedCats;
    CachedRepo.saveOnboarding(user);
    onboardingState.delete(userId);

    await ctx.editMessageText(
        `✅ <b>Отлично! Выбрано тем: ${user.categories.length}</b>\n\n` +
        user.categories.map(c => CATEGORIES[c].name).join('  '),
        { parse_mode: 'HTML' }
    ).catch(() => {});

    await ctx.reply(
        `📍 <b>Шаг 2 из 2 — Регион (необязательно)</b>\n\nДобавить региональные новости? Можно настроить позже в меню.`,
        { parse_mode: 'HTML', ...Markup.inlineKeyboard([
            [Markup.button.callback('📍 Выбрать регион', 'ob_go_regions')],
            [Markup.button.callback('⏭ Пропустить', 'ob_skip_regions')]
        ])}
    );
});

bot.action('ob_none', ctx => ctx.answerCbQuery('Выберите хотя бы одну тему').catch(() => {}));

bot.action('ob_go_regions', async ctx => {
    ctx.answerCbQuery().catch(() => {});
    const user = CachedRepo.get(ctx.from.id);
    await ctx.editMessageText('📍 Выберите интересные вам регионы:', getRegionsMenu(user)).catch(() => {});
    // После регионов — финальное сообщение
    await ctx.reply(
        `🚀 <b>Всё готово!</b>\n\nТвоя лента настроена. Нажми кнопку ниже чтобы получить первую новость!`,
        { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('⚡️ Первая новость!', 'next')]])}
    );
});

bot.action('ob_skip_regions', async ctx => {
    ctx.answerCbQuery().catch(() => {});
    await ctx.editMessageText(
        `🚀 <b>Всё готово!</b>\n\nТвоя лента настроена. Нажми кнопку ниже чтобы получить первую новость!`,
        { parse_mode: 'HTML', ...Markup.inlineKeyboard([[Markup.button.callback('⚡️ Первая новость!', 'next')]])}
    ).catch(() => {});
});
// ---- КОНЕЦ ОНБОРДИНГА ----

bot.hears(['📱 Меню', '📱 Главное меню настроек', '/menu'], ctx => ctx.reply('Главное меню:', getMenu(CachedRepo.get(ctx.from.id))));

const adminState = {}; // legacy, не используется
bot.command('admin', ctx => {
    if (!ADMIN_ID || String(ctx.from.id) !== ADMIN_ID) return ctx.reply('⛔ У вас нет доступа.');

    const now = Date.now();
    const h24 = now - 86400000;
    const h48 = now - 172800000;
    const day7 = now - 7 * 86400000;
    const day30 = now - 30 * 86400000;

    // Пользователи
    const usersTotal   = db.prepare('SELECT COUNT(*) as c FROM users').get().c;
    const usersActive  = db.prepare("SELECT COUNT(*) as c FROM users WHERE json_extract(notification_settings, '$.mode') != 'off'").get().c;
    const usersSmart   = db.prepare("SELECT COUNT(*) as c FROM users WHERE json_extract(notification_settings, '$.mode') = 'smart'").get().c;
    const usersCustom  = db.prepare("SELECT COUNT(*) as c FROM users WHERE json_extract(notification_settings, '$.mode') = 'custom'").get().c;
    const usersOff     = db.prepare("SELECT COUNT(*) as c FROM users WHERE json_extract(notification_settings, '$.mode') = 'off'").get().c;
    const usersNew24h  = db.prepare(`SELECT COUNT(*) as c FROM users WHERE CAST(strftime('%s', created_at) AS INTEGER) >= ?`).get(Math.floor(h24 / 1000)).c;
    const usersNew7d   = db.prepare(`SELECT COUNT(*) as c FROM users WHERE CAST(strftime('%s', created_at) AS INTEGER) >= ?`).get(Math.floor(day7 / 1000)).c;
    const usersNew30d  = db.prepare(`SELECT COUNT(*) as c FROM users WHERE CAST(strftime('%s', created_at) AS INTEGER) >= ?`).get(Math.floor(day30 / 1000)).c;

    // Вовлечённость (seen_log = факт прочтения)
    const reads24h = db.prepare('SELECT COUNT(DISTINCT user_id) as c FROM seen_log sl JOIN news n ON sl.news_id = n.id WHERE n.published_at >= ?').get(h24).c;
    const reads7d  = db.prepare('SELECT COUNT(DISTINCT user_id) as c FROM seen_log sl JOIN news n ON sl.news_id = n.id WHERE n.published_at >= ?').get(day7).c;
    const totalReads = db.prepare('SELECT COUNT(*) as c FROM seen_log').get().c;

    // Новости
    const newsTotal   = db.prepare('SELECT COUNT(*) as c FROM news').get().c;
    const news24h     = db.prepare('SELECT COUNT(*) as c FROM news WHERE published_at >= ?').get(h24).c;
    const news7d      = db.prepare('SELECT COUNT(*) as c FROM news WHERE published_at >= ?').get(day7).c;

    // Топ категорий по количеству подписчиков
    const catStats = Object.entries(CATEGORIES).map(([key, cat]) => {
        const count = db.prepare(`SELECT COUNT(*) as c FROM users WHERE categories LIKE ?`).get(`%"${key}"%`).c;
        return { name: cat.name, count };
    }).sort((a, b) => b.count - a.count);

    // Все регионы с количеством подписчиков
    const regStats = Object.entries(REGIONS).map(([key, reg]) => {
        const count = db.prepare(`SELECT COUNT(*) as c FROM users WHERE regions LIKE ?`).get(`%"${key}"%`).c;
        return { name: reg.name, count };
    }).sort((a, b) => b.count - a.count);
    const usersWithRegion = db.prepare(`SELECT COUNT(*) as c FROM users WHERE regions != '[]'`).get().c;

    // Ночной режим / звук
    const nightOn  = db.prepare("SELECT COUNT(*) as c FROM users WHERE json_extract(notification_settings, '$.night_mode') = 1").get().c;
    const silentOn = db.prepare("SELECT COUNT(*) as c FROM users WHERE json_extract(notification_settings, '$.silent') = 1").get().c;

    const activeRate = usersTotal ? Math.round(usersActive / usersTotal * 100) : 0;
    const dau = reads24h;
    const wau = reads7d;

    const catLines = catStats.map(c => `  ${c.name}: <b>${c.count}</b>`).join('\n');
    const regLines = regStats.filter(r => r.count > 0).length
        ? regStats.filter(r => r.count > 0).map(r => `  ${r.name}: <b>${r.count}</b>`).join('\n')
        : '  —';

    const msg =
`📊 <b>Flash News — Медиастатистика</b>
━━━━━━━━━━━━━━━━━━━

👥 <b>АУДИТОРИЯ</b>
  Всего пользователей: <b>${usersTotal}</b>
  Активных (рассылка вкл): <b>${usersActive}</b> (${activeRate}%)
  ├ Умная лента: <b>${usersSmart}</b>
  ├ По расписанию: <b>${usersCustom}</b>
  └ Отключена: <b>${usersOff}</b>

📈 <b>ПРИРОСТ</b>
  За 24 часа: <b>+${usersNew24h}</b>
  За 7 дней: <b>+${usersNew7d}</b>
  За 30 дней: <b>+${usersNew30d}</b>

🔥 <b>ВОВЛЕЧЁННОСТЬ</b>
  DAU (читали сегодня): <b>${dau}</b>
  WAU (читали за 7 дней): <b>${wau}</b>
  Всего прочтений: <b>${totalReads}</b>
  🌙 Ночной режим вкл: <b>${nightOn}</b>
  🔇 Тихий режим вкл: <b>${silentOn}</b>

📰 <b>КОНТЕНТ</b>
  Новостей в базе: <b>${newsTotal}</b>
  Добавлено за 24ч: <b>${news24h}</b>
  Добавлено за 7 дней: <b>${news7d}</b>

🗂 <b>ИНТЕРЕСЫ АУДИТОРИИ</b>
${catLines}

📍 <b>РЕГИОНЫ</b> (подписчиков с регионом: <b>${usersWithRegion}</b>)
${regLines}
━━━━━━━━━━━━━━━━━━━
<i>Данные актуальны на момент запроса</i>`;

    ctx.reply(msg, { parse_mode: 'HTML' });
});

// /stats — красивая выжимка для рекламодателей (без лишних деталей)
bot.command('stats', ctx => {
    if (!ADMIN_ID || String(ctx.from.id) !== ADMIN_ID) return ctx.reply('⛔ У вас нет доступа.');

    const now = Date.now();
    const h24  = now - 86400000;
    const day7  = now - 7 * 86400000;
    const day30 = now - 30 * 86400000;

    const usersTotal  = db.prepare('SELECT COUNT(*) as c FROM users').get().c;
    const usersActive = db.prepare("SELECT COUNT(*) as c FROM users WHERE json_extract(notification_settings, '$.mode') != 'off'").get().c;
    const usersNew7d  = db.prepare(`SELECT COUNT(*) as c FROM users WHERE CAST(strftime('%s', created_at) AS INTEGER) >= ?`).get(Math.floor(day7 / 1000)).c;
    const usersNew30d = db.prepare(`SELECT COUNT(*) as c FROM users WHERE CAST(strftime('%s', created_at) AS INTEGER) >= ?`).get(Math.floor(day30 / 1000)).c;
    const dau = db.prepare('SELECT COUNT(DISTINCT user_id) as c FROM seen_log sl JOIN news n ON sl.news_id = n.id WHERE n.published_at >= ?').get(h24).c;
    const wau = db.prepare('SELECT COUNT(DISTINCT user_id) as c FROM seen_log sl JOIN news n ON sl.news_id = n.id WHERE n.published_at >= ?').get(day7).c;
    const activeRate = usersTotal ? Math.round(usersActive / usersTotal * 100) : 0;
    const dauRate = usersTotal ? Math.round(dau / usersTotal * 100) : 0;

    const catStats = Object.entries(CATEGORIES).map(([key, cat]) => {
        const count = db.prepare(`SELECT COUNT(*) as c FROM users WHERE categories LIKE ?`).get(`%"${key}"%`).c;
        return { name: cat.name, count };
    }).sort((a, b) => b.count - a.count);
    const catLines = catStats.map(c => `${c.name} — <b>${c.count}</b> подп.`).join('\n');

    const today = new Date().toLocaleDateString('ru-RU', { day: 'numeric', month: 'long', year: 'numeric' });

    const msg =
`⚡️ <b>Flash News — Медиакит</b>
<i>на ${today}</i>
━━━━━━━━━━━━━━━━━━━

Flash News — Telegram-бот агрегатор новостей на русском языке. Доставляет актуальные новости по интересам напрямую в мессенджер.

👥 <b>Аудитория</b>
  Подписчиков всего: <b>${usersTotal}</b>
  С активной рассылкой: <b>${usersActive}</b> (${activeRate}%)
  Новых за 7 дней: <b>+${usersNew7d}</b>
  Новых за 30 дней: <b>+${usersNew30d}</b>

📊 <b>Активность</b>
  DAU: <b>${dau}</b> (${dauRate}% от базы)
  WAU: <b>${wau}</b>

🗂 <b>Тематические сегменты</b>
${catLines}

💡 <b>Форматы размещения</b>
  • Нативный пост в рассылке
  • Закреплённый пост в ленте
  • Упоминание в новости

📩 По вопросам размещения: @ваш_контакт
━━━━━━━━━━━━━━━━━━━`;

    ctx.reply(msg, { parse_mode: 'HTML' });
});

bot.command('clean_sources', ctx => {
    if (!ADMIN_ID || String(ctx.from.id) !== ADMIN_ID) return;

    // Актуальные источники из конфига
    const activeSources = new Set();
    Object.values(CATEGORIES).forEach(cat => {
        cat.urls.forEach(src => activeSources.add(src.n));
    });
    Object.values(REGIONS).forEach(reg => {
        reg.urls.forEach(url => {
            // Региональные источники используют name региона
        });
        activeSources.add(reg.name);
    });

    // Находим источники которых нет в конфиге
    const allSources = db.prepare('SELECT DISTINCT source_name FROM news').all().map(r => r.source_name);
    const deadSources = allSources.filter(s => !activeSources.has(s));

    if (deadSources.length === 0) return ctx.reply('✅ Мёртвых источников не найдено.');

    const placeholders = deadSources.map(() => '?').join(',');
    const res1 = db.prepare(`DELETE FROM news WHERE source_name IN (${placeholders})`).run(...deadSources);
    const res2 = db.prepare('DELETE FROM seen_log WHERE news_id NOT IN (SELECT id FROM news)').run();

    ctx.reply(
        `🧹 <b>Мёртвые источники удалены</b>\n\n` +
        `Источники: <i>${deadSources.join(', ')}</i>\n` +
        `Удалено новостей: <b>${res1.changes}</b>\n` +
        `Очищено логов: <b>${res2.changes}</b>`,
        { parse_mode: 'HTML' }
    );
});

bot.command('clean_db', ctx => {
    if (!ADMIN_ID || String(ctx.from.id) !== ADMIN_ID) return;
    const cutoffDate = Date.now() - (CONFIG.NEWS_TTL_DAYS * 24 * 60 * 60 * 1000);
    const res1 = db.prepare('DELETE FROM news WHERE published_at < ?').run(cutoffDate);
    const res2 = db.prepare('DELETE FROM seen_log WHERE news_id NOT IN (SELECT id FROM news)').run();
    MediaCache.clear();
    ctx.reply(`🧹 <b>База очищена!</b>\n\nУдалено старых новостей: <b>${res1.changes}</b>\nУдалено логов просмотров: <b>${res2.changes}</b>`, { parse_mode: 'HTML' });
});

bot.action('set_mode_smart', async ctx => {
    const user = CachedRepo.get(ctx.from.id); user.notification_settings.mode = 'smart'; CachedRepo.saveSettings(user);
    ctx.answerCbQuery('Умная лента включена').catch(() => {});
    ctx.editMessageText(getSettingsText('smart'), { parse_mode: 'HTML', ...getSettingsMenu(user) }).catch(() => {});
});
bot.action('set_mode_custom', async ctx => {
    const user = CachedRepo.get(ctx.from.id); user.notification_settings.mode = 'custom'; CachedRepo.saveSettings(user);
    ctx.answerCbQuery('Включена рассылка по расписанию').catch(() => {});
    ctx.editMessageText(getSettingsText('custom'), { parse_mode: 'HTML', ...getSettingsMenu(user) }).catch(() => {});
});
bot.action('set_mode_off', async ctx => {
    const user = CachedRepo.get(ctx.from.id); user.notification_settings.mode = 'off'; CachedRepo.saveSettings(user);
    ctx.answerCbQuery('Рассылка отключена').catch(() => {});
    ctx.editMessageText(getSettingsText('off'), { parse_mode: 'HTML', ...getSettingsMenu(user) }).catch(() => {});
});
bot.action('toggle_silent', async ctx => {
    const user = CachedRepo.get(ctx.from.id); user.notification_settings.silent = !user.notification_settings.silent; CachedRepo.saveSettings(user);
    ctx.answerCbQuery('Звук изменён').catch(() => {});
    ctx.editMessageReplyMarkup(getSettingsMenu(user).reply_markup).catch(() => {});
});
bot.action('toggle_night', async ctx => {
    const user = CachedRepo.get(ctx.from.id); user.notification_settings.night_mode = !user.notification_settings.night_mode; CachedRepo.saveSettings(user);
    ctx.answerCbQuery('Ночной режим изменён').catch(() => {});
    ctx.editMessageReplyMarkup(getSettingsMenu(user).reply_markup).catch(() => {});
});
bot.action('cycle_interval', async ctx => {
    const user = CachedRepo.get(ctx.from.id); const v = [0.5, 1, 2, 4, 6, 8, 12, 24]; user.notification_settings.interval = v[(v.indexOf(user.notification_settings.interval) + 1) % v.length] || 0.5;
    CachedRepo.saveSettings(user); ctx.answerCbQuery('Интервал изменён').catch(() => {}); ctx.editMessageReplyMarkup(getSettingsMenu(user).reply_markup).catch(() => {});
});
bot.action('cycle_limit', async ctx => {
    const user = CachedRepo.get(ctx.from.id); const v = [1, 2, 3, 5, 10]; user.notification_settings.limit = v[(v.indexOf(user.notification_settings.limit) + 1) % v.length] || 1;
    CachedRepo.saveSettings(user); ctx.answerCbQuery('Лимит изменён').catch(() => {}); ctx.editMessageReplyMarkup(getSettingsMenu(user).reply_markup).catch(() => {});
});
bot.action('menu_settings', async ctx => {
    const user = CachedRepo.get(ctx.from.id);
    ctx.answerCbQuery().catch(() => {}); ctx.editMessageText(getSettingsText(user.notification_settings.mode), { parse_mode: 'HTML', ...getSettingsMenu(user) }).catch(() => {});
});
bot.action('menu_reg', async ctx => {
    const user = CachedRepo.get(ctx.from.id);
    ctx.answerCbQuery().catch(() => {}); ctx.editMessageText('📍 Выберите интересные вам регионы:', getRegionsMenu(user)).catch(() => {});
});
bot.action(/toggle_reg_(.+)/, async ctx => {
    const reg = ctx.match[1]; const user = CachedRepo.get(ctx.from.id);
    user.regions = user.regions.includes(reg) ? user.regions.filter(r => r !== reg) : [...user.regions, reg];
    CachedRepo.saveRegions(user); ctx.answerCbQuery().catch(() => {}); ctx.editMessageReplyMarkup(getRegionsMenu(user).reply_markup).catch(() => {});
});
bot.action(/toggle_cat_(.+)/, async ctx => {
    const cat = ctx.match[1]; const user = CachedRepo.get(ctx.from.id);
    user.categories = user.categories.includes(cat) ? user.categories.filter(c => c !== cat) : [...user.categories, cat];
    CachedRepo.saveCategories(user); ctx.answerCbQuery().catch(() => {}); ctx.editMessageReplyMarkup(getMenu(user).reply_markup).catch(() => {});
});
bot.action('back_main', async ctx => {
    const user = CachedRepo.get(ctx.from.id);
    ctx.answerCbQuery().catch(() => {}); ctx.editMessageText('Главное меню:', getMenu(user)).catch(() => {});
});
bot.action('show_info', async ctx => {
    ctx.answerCbQuery().catch(() => {});
    ctx.reply('ℹ️ <b>О проекте</b>\n\nБот является автоматическим поисковым агрегатором.', { parse_mode: 'HTML' });
});

bot.action('next', ctx => {
    ctx.answerCbQuery().catch(() => {});
    serveNextNews(ctx).catch(err => logger.error(`Next button error: ${err.message}`));
});

function smartTruncate(text, limit = 600) {
    if (text.length <= limit) return text;
    let cut = text.slice(0, limit);
    let lastPunc = Math.max(cut.lastIndexOf('. '), cut.lastIndexOf('! '), cut.lastIndexOf('? '), cut.lastIndexOf('.\n'), cut.lastIndexOf('!'), cut.lastIndexOf('?'));
    if (lastPunc > limit * 0.5) return cut.slice(0, lastPunc + 1);
    let lastSpace = cut.lastIndexOf(' ');
    if (lastSpace > 0) return cut.slice(0, lastSpace) + '...';
    return cut + '...';
}

async function sendRaw(user, news, isManual = false, isSmart = false) {
    const safeTitle = escapeHTML(news.title);
    const safeSource = escapeHTML(news.source_name);
    let footer = `\n\n🔹 Источник: <a href="${news.link}">${safeSource}</a>`;
    let merged = []; try { merged = JSON.parse(news.merged_sources || '[]'); } catch(e){}
    if (merged.length > 0) footer += `\n🗣 <i>Также пишут: ${escapeHTML(merged.join(', '))}</i>`;
    if (isSmart) footer += `\n⚡️ <i>#ГЛАВНОЕ</i>`;

    let finalBody = smartTruncate(news.body, 400);
    finalBody = escapeHTML(finalBody);

    let hasVideo = !!news.video_url;
    let imgUrl = news.image_url;

    let caption = `<b>${safeTitle}</b>\n\n${finalBody}`;
    if (hasVideo) caption += `\n\n🎥 <i>(Видео доступно в оригинальном посте)</i>`;
    caption += footer;

    const kb = Markup.inlineKeyboard([[Markup.button.callback('🔄 Следующая новость', 'next')]]).reply_markup;
    const extra = { parse_mode: 'HTML', reply_markup: kb, disable_notification: !isManual && user.notification_settings.silent };

    if (imgUrl) {
        let cachedFileId = MediaCache.get(imgUrl);
        try {
            if (cachedFileId) {
                await telegramDispatcher.enqueue(() => bot.telegram.sendPhoto(user.id, cachedFileId, { ...extra, caption }));
                db.prepare('INSERT OR IGNORE INTO seen_log VALUES (?, ?)').run(user.id, news.id);
                return;
            }
            // Сначала пробуем URL напрямую
            const res = await telegramDispatcher.enqueue(() => bot.telegram.sendPhoto(user.id, imgUrl, { ...extra, caption }));
            if (res && res.photo) {
                MediaCache.set(imgUrl, res.photo[res.photo.length - 1].file_id);
            }
        } catch (err) {
            // URL не сработал — качаем буфером с правильными заголовками
            try {
                const resp = await axios.get(imgUrl, {
                    headers: {
                        ...REQUEST_HEADERS,
                        'Referer': new URL(imgUrl).origin,
                        'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
                    },
                    responseType: 'arraybuffer',
                    timeout: 12000
                });
                const res = await telegramDispatcher.enqueue(() => bot.telegram.sendPhoto(user.id, { source: Buffer.from(resp.data) }, { ...extra, caption }));
                if (res && res.photo) {
                    MediaCache.set(imgUrl, res.photo[res.photo.length - 1].file_id);
                }
            } catch (e2) {
                // Картинка недоступна — отправляем новость без фото
                logger.warn(`[Image Fail] Sending without photo: ${e2.message}`);
                try { await telegramDispatcher.enqueue(() => bot.telegram.sendMessage(user.id, caption, { ...extra, link_preview_options: { is_disabled: true } })); } catch(e){}
            }
        }
    } else {
        try { await telegramDispatcher.enqueue(() => bot.telegram.sendMessage(user.id, caption, { ...extra, link_preview_options: { is_disabled: true } })); } catch(e){}
    }

    db.prepare('INSERT OR IGNORE INTO seen_log VALUES (?, ?)').run(user.id, news.id);
}

function pickStrictDiverseNews(batch, user) {
    const history = user.notification_settings.source_history || [];
    // Ищем новость не из последних 5 источников
    let idx = batch.findIndex(n => !history.includes(n.source_name));
    if (idx === -1) idx = 0; // все источники в истории — берём первую
    const news = batch.splice(idx, 1)[0];
    if (!news) return null;
    // Обновляем историю источников
    history.push(news.source_name);
    if (history.length > 5) history.shift();
    user.notification_settings.source_history = history;
    user.notification_settings.last_source = news.source_name;
    return news;
}

async function serveNextNews(ctx) {
    const user = CachedRepo.get(ctx.from.id);
    const batch = Repo.getUnseen(user.id, user.categories, user.regions, 50);
    if (!batch.length) {
        if (ctx.callbackQuery) return; else return ctx.reply('📭 Пока новых новостей нет.');
    }
    const selectedNews = pickStrictDiverseNews(batch, user);
    if (!selectedNews) {
        if (ctx.callbackQuery) return; else return ctx.reply('⏳ Ждем посты от других изданий!');
    }
    db.prepare('UPDATE users SET notification_settings=? WHERE id=?').run(JSON.stringify(user.notification_settings), user.id);
    await sendRaw(user, selectedNews, true);
}

const AutoMailer = {
    _running: false,
    async run() {
        if (this._running) return;
        this._running = true;
        try {
        const now = Date.now(); const currentHourMSK = (new Date().getUTCHours() + 3) % 24;
        let lastId = 0;
        while (true) {
            const users = db.prepare(`SELECT * FROM users WHERE id > ? AND json_extract(notification_settings, '$.mode') != 'off' ORDER BY id LIMIT ?`).all(lastId, CONFIG.MAILER_BATCH_SIZE);
            if (!users.length) break;
            for (const rawUser of users) {
                lastId = rawUser.id;
            let s; try { s = JSON.parse(rawUser.notification_settings || '{}'); } catch(e) { continue; }
            const user = {
                ...rawUser, categories: (() => { try { return JSON.parse(rawUser.categories || '[]'); } catch { return []; } })(),
                regions: (() => { try { return JSON.parse(rawUser.regions || '[]'); } catch { return []; } })(),
                notification_settings: {
                    mode: s.mode || 'smart', interval: s.interval || 4, limit: s.limit || 3, silent: s.silent || false,
                    last_sent: s.last_sent || 0, last_source: s.last_source || '', night_mode: s.night_mode !== undefined ? s.night_mode : true, smart_history: s.smart_history || []
                }
            };
            if (user.notification_settings.mode === 'off' || (user.notification_settings.night_mode && (currentHourMSK >= 23 || currentHourMSK < 8))) continue;

            if (user.notification_settings.mode === 'custom') {
                if (now - user.notification_settings.last_sent >= user.notification_settings.interval * 3600000) {
                    const batch = Repo.getUnseen(user.id, user.categories, user.regions, user.notification_settings.limit * 5);
                    let sentCount = 0;
                    for (let j = 0; j < batch.length && sentCount < user.notification_settings.limit; j++) {
                        const news = pickStrictDiverseNews(batch, user); if (!news) break;
                        await sendRaw(user, news, false); sentCount++; await sleep(1100);
                    }
                    if (sentCount > 0) {
                        user.notification_settings.last_sent = now;
                        db.prepare('UPDATE users SET notification_settings=? WHERE id=?').run(JSON.stringify(user.notification_settings), user.id);
                    }
                }
            } else if (user.notification_settings.mode === 'smart') {
                user.notification_settings.smart_history = (user.notification_settings.smart_history || []).filter(timestamp => now - timestamp < 7200000);
                if (user.notification_settings.smart_history.length >= 5) continue;
                const conditions = []; const args = [];
                if (user.categories.length) { conditions.push(`category IN (${user.categories.map(()=>'?').join(',')})`); args.push(...user.categories); }
                if (user.regions.length) { conditions.push(`region IN (${user.regions.map(()=>'?').join(',')})`); args.push(...user.regions); }
                if (!conditions.length) continue;
                const topNews = db.prepare(`SELECT * FROM news WHERE (${conditions.join(' OR ')}) AND merged_sources != '[]' AND length(body) > 50 AND id NOT IN (SELECT news_id FROM seen_log WHERE user_id=?) ORDER BY published_at DESC LIMIT 1`).get(...args, user.id);
                if (topNews && topNews.source_name !== user.notification_settings.last_source) {
                    await sendRaw(user, topNews, false, true);
                    user.notification_settings.last_source = topNews.source_name; user.notification_settings.smart_history.push(now);
                    db.prepare('UPDATE users SET notification_settings=? WHERE id=?').run(JSON.stringify(user.notification_settings), user.id);
                }
            }
        }
        }
        } finally {
            this._running = false;
        }
    }
};

const ingestTask = cron.schedule('*/5 * * * *', () => { void Ingester.run(); });
const mailerTask = cron.schedule('* * * * *', () => { void AutoMailer.run(); });
const cleanupTask = cron.schedule('0 3 * * *', () => {
    const cutoffDate = Date.now() - (CONFIG.NEWS_TTL_DAYS * 24 * 60 * 60 * 1000);
    db.prepare('DELETE FROM news WHERE published_at < ?').run(cutoffDate);
    db.prepare('DELETE FROM seen_log WHERE news_id NOT IN (SELECT id FROM news)').run();
    db.prepare('DELETE FROM seen_log WHERE rowid NOT IN (SELECT rowid FROM seen_log ORDER BY news_id DESC LIMIT 3000000)').run();
    // Удаляем старые записи кэша медиа (старше 7 дней)
    db.prepare('DELETE FROM media_cache WHERE saved_at < ?').run(Date.now() - 7 * 86400000);
    _mediaCacheMap.clear();
});

// ---- ТАРГЕТИРОВАННАЯ РАССЫЛКА ----
const broadcastState = new Map(); // adminId -> { targetCats, messageId, chatId }

function getBroadcastTargetMenu(selectedCats) {
    const keys = Object.keys(CATEGORIES);
    const btns = [];
    for (let i = 0; i < keys.length; i += 2) {
        const row = [Markup.button.callback(
            `${selectedCats.includes(keys[i]) ? '✅' : '⬜'} ${CATEGORIES[keys[i]].name}`,
            `bc_cat_${keys[i]}`
        )];
        if (keys[i + 1]) row.push(Markup.button.callback(
            `${selectedCats.includes(keys[i + 1]) ? '✅' : '⬜'} ${CATEGORIES[keys[i + 1]].name}`,
            `bc_cat_${keys[i + 1]}`
        ));
        btns.push(row);
    }
    btns.push([
        Markup.button.callback('👥 Всем', 'bc_cat_ALL'),
        Markup.button.callback(selectedCats.length > 0 || selectedCats[0] === 'ALL' ? '✅ Выбрано' : '⬜ Подтвердить', 'bc_confirm')
    ]);
    btns.push([Markup.button.callback('❌ Отмена', 'bc_cancel')]);
    return Markup.inlineKeyboard(btns);
}

bot.command('broadcast', async ctx => {
    if (!ADMIN_ID || String(ctx.from.id) !== ADMIN_ID) return ctx.reply('⛔ Нет доступа.');
    broadcastState.set(ctx.from.id, { targetCats: [], pendingMsgId: null });
    pruneMap(broadcastState, CONFIG.MAX_EPHEMERAL_STATE);
    ctx.reply(
        `📣 <b>Новая рассылка</b>\n\nВыбери аудиторию — кому отправить сообщение:`,
        { parse_mode: 'HTML', ...getBroadcastTargetMenu([]) }
    );
});

bot.action(/bc_cat_(.+)/, async ctx => {
    if (!ADMIN_ID || String(ctx.from.id) !== ADMIN_ID) return ctx.answerCbQuery().catch(() => {});
    ctx.answerCbQuery().catch(() => {});
    const cat = ctx.match[1];
    const state = broadcastState.get(ctx.from.id) || { targetCats: [] };

    if (cat === 'ALL') {
        state.targetCats = ['ALL'];
    } else {
        state.targetCats = state.targetCats.filter(c => c !== 'ALL');
        state.targetCats = state.targetCats.includes(cat)
            ? state.targetCats.filter(c => c !== cat)
            : [...state.targetCats, cat];
    }
    broadcastState.set(ctx.from.id, state);
    ctx.editMessageReplyMarkup(getBroadcastTargetMenu(state.targetCats).reply_markup).catch(() => {});
});

bot.action('bc_confirm', async ctx => {
    if (!ADMIN_ID || String(ctx.from.id) !== ADMIN_ID) return ctx.answerCbQuery().catch(() => {});
    const state = broadcastState.get(ctx.from.id);
    if (!state || state.targetCats.length === 0) return ctx.answerCbQuery('Выберите аудиторию!').catch(() => {});
    ctx.answerCbQuery().catch(() => {});

    const isAll = state.targetCats.includes('ALL');
    let audienceCount;
    if (isAll) {
        audienceCount = db.prepare('SELECT COUNT(*) as c FROM users').get().c;
    } else {
        const conditions = state.targetCats.map(() => 'categories LIKE ?').join(' OR ');
        const params = state.targetCats.map(c => `%"${c}"%`);
        audienceCount = db.prepare(`SELECT COUNT(*) as c FROM users WHERE ${conditions}`).get(...params).c;
    }

    const catNames = isAll ? 'Все пользователи' : state.targetCats.map(c => CATEGORIES[c]?.name || c).join(', ');
    state.confirmed = true;
    broadcastState.set(ctx.from.id, state);

    await ctx.editMessageText(
        `📣 <b>Рассылка подтверждена</b>\n\n` +
        `🎯 Аудитория: <b>${catNames}</b>\n` +
        `👥 Получателей: ~<b>${audienceCount}</b> чел.\n\n` +
        `Отправь мне сообщение (текст, фото, видео) — оно разлетится по выбранной аудитории.\n` +
        `Для отмены: /cancel`,
        { parse_mode: 'HTML' }
    ).catch(() => {});
});

bot.action('bc_cancel', async ctx => {
    if (!ADMIN_ID || String(ctx.from.id) !== ADMIN_ID) return ctx.answerCbQuery().catch(() => {});
    broadcastState.delete(ctx.from.id);
    ctx.answerCbQuery('Отменено').catch(() => {});
    ctx.editMessageText('❌ Рассылка отменена.').catch(() => {});
});

bot.command('cancel', ctx => {
    if (!ADMIN_ID || String(ctx.from.id) !== ADMIN_ID) return;
    broadcastState.delete(ctx.from.id);
    ctx.reply('❌ Рассылка отменена.');
});

bot.on('message', async (ctx, next) => {
    if (!ADMIN_ID || String(ctx.from.id) !== ADMIN_ID) return next();
    if (ctx.message.text && ctx.message.text.startsWith('/')) return next();

    const state = broadcastState.get(ctx.from.id);
    if (!state || !state.confirmed) return next();

    broadcastState.delete(ctx.from.id);
    const isAll = state.targetCats.includes('ALL');

    // Получаем аудиторию
    let userIds;
    if (isAll) {
        userIds = db.prepare('SELECT id FROM users').all().map(u => u.id);
    } else {
        const conditions = state.targetCats.map(() => 'categories LIKE ?').join(' OR ');
        const params = state.targetCats.map(c => `%"${c}"%`);
        userIds = db.prepare(`SELECT id FROM users WHERE ${conditions}`).all(...params).map(u => u.id);
    }

    const statusMsg = await ctx.reply(`⏳ Начинаю рассылку на ${userIds.length} получателей...`);
    let success = 0, failed = 0;

    for (const uid of userIds) {
        try {
            await telegramDispatcher.enqueue(() => ctx.telegram.copyMessage(uid, ctx.from.id, ctx.message.message_id));
            success++;
        } catch (err) { failed++; }
    }

    ctx.telegram.editMessageText(
        ctx.chat.id, statusMsg.message_id, undefined,
        `✅ <b>Рассылка завершена!</b>\n\n` +
        `🎯 Аудитория: ${isAll ? 'Все' : state.targetCats.map(c => CATEGORIES[c]?.name).join(', ')}\n` +
        `✉️ Доставлено: <b>${success}</b>\n` +
        `❌ Ошибок: <b>${failed}</b>`,
        { parse_mode: 'HTML' }
    ).catch(() => {});
});

(async () => {
    logger.info('Flash News v73.0 started. Admin ID: ' + ADMIN_ID);
    await Ingester.run();
    await bot.launch();
    setupGracefulShutdown({
        logger,
        bot,
        db,
        telegramDispatcher,
        cronTasks: [ingestTask, mailerTask, cleanupTask]
    });
})();