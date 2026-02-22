import React, { useState, useEffect, useCallback, useMemo } from "react";
import {
    ButtonItem,
    PanelSection,
    PanelSectionRow,
    TextField,
    Focusable,
    DropdownItem,
    showContextMenu,
    Menu,
    MenuItem,
    staticClasses,
} from "@decky/ui";
import { definePlugin, call } from "@decky/api";

const BUILD = "v3.1.15";

// ── Translations ───────────────────────────────────────────────────────────
const TRANSLATIONS: Record<string, Record<string, string>> = {
    ru: {
        tab_wishlist: "Желаемое",
        tab_sales: "Скидки",
        tab_settings: "Настройки",
        btn_refresh: "Обновить список",
        btn_loading: "⏳ Загрузка...",
        btn_saving: "⏳ Сохраняю...",
        btn_saved: "✅ Сохранено!",
        btn_save_id: "Сохранить Steam ID",
        txt_no_games: "Нет игр",
        txt_no_sales: "Нет предстоящих распродаж",
        lbl_currency: "Валюта",
        lbl_language: "Язык",
        lbl_region: "Регион магазина Steam",
        btn_update_rates: "Обновить курсы валют",
        msg_rates_ok: "✅ Курсы валют обновлены",
        msg_rates_err: "⚠️ Ошибка обновления курсов",
        desc_steamid: "steamid.io или steamidfinder.com",
        txt_not_loaded: "цена не загружена",
        txt_min: "мин",
        txt_no_img: "нет фото",
        msg_sync_steam_id: "⚠️ Steam ID не указан",
        msg_sync_empty: "⚠️ Список пуст или скрыт (приватность)",
        msg_sync_private: "⚠️ Список скрыт (только для друзей)",
        msg_sync_limit: "⚠️ Лимит запросов. Подождите.",
        msg_sync_html: "⚠️ Steam вернул HTML (список скрыт?)",
        msg_sync_net: "⚠️ Ошибка сети",
        msg_click_refresh: "Нажмите «Обновить список»",
        msg_enter_id: "Укажите Steam ID",
        msg_cache: "Кэш:",
        msg_loaded: "Загружено игр:",
        lbl_current_price: "Текущая цена",
        lbl_standard_price: "Стандартная цена",
        lbl_hist_low: "Исторический минимум",
        lbl_metacritic: "Metacritic",
        lbl_verdict_low: "🔥 ИСТ. МИНИМУМ",
        lbl_verdict_potato: "🥔 КАРТОШЕЧНАЯ СКИДКА",
        lbl_verdict_wait: "⏳ ЛУЧШЕ ПОДОЖДАТЬ",
        lbl_sort_by: "Сортировка",
        sort_discount: "По снижению скидки",
        sort_price_asc: "Сначала дешевые",
        sort_price_desc: "Сначала дорогие",
        sort_title_asc: "По алфавиту",
        sort_date_added: "По дате добавления",
        sort_metacritic: "По Metacritic",
        sales_progress: "ПРОГРЕСС",
        sales_live_now: "ИДЁТ СЕЙЧАС",
        sales_confirmed: "OK",
        sales_predicted: "ПРОГНОЗ",
        sales_disclaimer: "Данные основаны на официальных анонсах Valve и исторических циклах Steam.",
        lbl_paste: "📋 Вставить из буфера",
        lbl_paste_fail: "⚠️ Буфер обмена недоступен",
        msg_save_error: "⚠️ Ошибка сохранения",
        msg_save_timeout: "⚠️ Сохранение заняло слишком долго",
        lbl_collecting: "Сбор данных…",
        lbl_single_point: "1 точка данных",
        lbl_no_verified_history: "Нет верифицированной истории цен",
        lbl_release_date: "Дата релиза",
        lbl_release_tba: "Дата релиза уточняется",
        lbl_history_unofficial: "SteamDB (неофиц.)",
        lbl_price_inconsistent: "Данные цены противоречивы",
        txt_page_not_avail: "Страница пока недоступна",
        txt_dates_unconfirmed: "Даты не подтверждены",
        txt_loading_notice: "Загрузка может занять до 2 минут",
        txt_dates_unavailable: "Даты недоступны"
    },
    en: {
        tab_wishlist: "Wishlist",
        tab_sales: "Sales",
        tab_settings: "Settings",
        btn_refresh: "Refresh List",
        btn_loading: "⏳ Loading...",
        btn_saving: "⏳ Saving...",
        btn_saved: "✅ Saved!",
        btn_save_id: "Save Steam ID",
        txt_no_games: "No games",
        txt_no_sales: "No upcoming sales",
        lbl_currency: "Currency",
        lbl_language: "Language",
        lbl_region: "Steam Store Region",
        btn_update_rates: "Update Exchange Rates",
        msg_rates_ok: "✅ Exchange rates updated",
        msg_rates_err: "⚠️ Error updating rates",
        desc_steamid: "steamid.io or steamidfinder.com",
        txt_not_loaded: "price not loaded",
        txt_min: "min",
        txt_no_img: "no img",
        msg_sync_steam_id: "⚠️ Steam ID not specified",
        msg_sync_empty: "⚠️ List is empty or private",
        msg_sync_private: "⚠️ List is private (friends only)",
        msg_sync_limit: "⚠️ Rate limit. Please wait.",
        msg_sync_html: "⚠️ Steam returned HTML (is list private?)",
        msg_sync_net: "⚠️ Network error",
        msg_click_refresh: "Click «Refresh List»",
        msg_enter_id: "Enter Steam ID",
        msg_cache: "Cache:",
        msg_loaded: "Games loaded:",
        lbl_current_price: "Current Price",
        lbl_standard_price: "Standard Price",
        lbl_hist_low: "Historical Low",
        lbl_metacritic: "Metacritic",
        lbl_verdict_low: "🔥 HISTORIC LOW",
        lbl_verdict_potato: "🥔 POTATO DEAL",
        lbl_verdict_wait: "⏳ WORTH THE WAIT",
        lbl_sort_by: "Sort",
        sort_discount: "Highest Discount",
        sort_price_asc: "Lowest Price",
        sort_price_desc: "Highest Price",
        sort_title_asc: "Alphabetical",
        sort_date_added: "Date Added",
        sort_metacritic: "Metacritic",
        sales_progress: "PROGRESS",
        sales_live_now: "LIVE NOW",
        sales_confirmed: "OK",
        sales_predicted: "PREDICTED",
        sales_disclaimer: "Data based on official Valve announcements and historical Steam cycles.",
        lbl_paste: "📋 Paste from clipboard",
        lbl_paste_fail: "⚠️ Clipboard not available",
        msg_save_error: "⚠️ Save error",
        msg_save_timeout: "⚠️ Save timed out",
        lbl_collecting: "Collecting data…",
        lbl_single_point: "1 data point",
        lbl_no_verified_history: "No verified price history",
        lbl_release_date: "Release Date",
        lbl_release_tba: "Release date: TBA",
        lbl_history_unofficial: "SteamDB (unofficial)",
        lbl_price_inconsistent: "Price data inconsistent",
        txt_page_not_avail: "Page not available yet",
        txt_dates_unconfirmed: "Dates not confirmed",
        txt_loading_notice: "Loading can take up to 2 minutes",
        txt_dates_unavailable: "Dates not available"
    },
    "zh-CN": {
        tab_wishlist: "愿望单",
        tab_sales: "特卖",
        tab_settings: "设置",
        btn_refresh: "刷新列表",
        btn_loading: "⏳ 加载中...",
        btn_saving: "⏳ 保存中...",
        btn_saved: "✅ 已保存!",
        btn_save_id: "保存 Steam ID",
        txt_no_games: "没有游戏",
        txt_no_sales: "没有即将到来的特卖",
        lbl_currency: "货币",
        lbl_language: "语言",
        lbl_region: "Steam 商店区域",
        btn_update_rates: "更新汇率",
        msg_rates_ok: "✅ 汇率已更新",
        msg_rates_err: "⚠️ 更新汇率时出错",
        desc_steamid: "steamid.io 或 steamidfinder.com",
        txt_not_loaded: "价格未加载",
        txt_min: "最低",
        txt_no_img: "无图片",
        msg_sync_steam_id: "⚠️ 未指定 Steam ID",
        msg_sync_empty: "⚠️ 愿望单为空或已隐藏",
        msg_sync_private: "⚠️ 愿望单为私密（仅限好友）",
        msg_sync_limit: "⚠️ 请求过于频繁，请稍候。",
        msg_sync_html: "⚠️ Steam 返回了 HTML (愿望单是否公开?)",
        msg_sync_net: "⚠️ 网络错误",
        msg_click_refresh: "点击「刷新列表」",
        msg_enter_id: "请输入 Steam ID",
        msg_cache: "缓存:",
        msg_loaded: "已加载游戏:",
        lbl_current_price: "当前价格",
        lbl_standard_price: "标准价格",
        lbl_hist_low: "史低价",
        lbl_metacritic: "Metacritic",
        lbl_verdict_low: "🔥 历史最低",
        lbl_verdict_potato: "🥔 超值特价",
        lbl_verdict_wait: "⏳ 值得等待",
        lbl_sort_by: "排序",
        sort_discount: "最高折扣",
        sort_price_asc: "价格从低到高",
        sort_price_desc: "价格从高到低",
        sort_title_asc: "按字母顺序",
        sort_date_added: "添加日期",
        sort_metacritic: "Metacritic",
        sales_progress: "进度",
        sales_live_now: "正在进行",
        sales_confirmed: "OK",
        sales_predicted: "预测",
        sales_disclaimer: "数据基于Valve官方公告和Steam历史周期。",
        lbl_paste: "📋 从剪贴板粘贴",
        lbl_paste_fail: "⚠️ 剪贴板不可用",
        msg_save_error: "⚠️ 保存错误",
        msg_save_timeout: "⚠️ 保存超时",
        lbl_collecting: "正在收集数据…",
        lbl_single_point: "1个数据点",
        lbl_no_verified_history: "暂无已验证价格历史",
        lbl_release_date: "发售日期",
        lbl_release_tba: "发售日期待定",
        lbl_history_unofficial: "SteamDB（非官方）",
        lbl_price_inconsistent: "价格数据不一致",
        txt_page_not_avail: "页面暂不可用",
        txt_dates_unconfirmed: "日期未确认",
        txt_loading_notice: "加载最多需要2分钟",
        txt_dates_unavailable: "日期不可用"
    }
};

function t(key: string, lang: string): string {
    const l = TRANSLATIONS[lang] || TRANSLATIONS["en"];
    return l[key] || TRANSLATIONS["en"][key] || key;
}

// ── Icon ───────────────────────────────────────────────────────────────────
const PotatoIcon = () => (
    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M7 5c-1.5 1-2.5 3-2.5 6s1 7 4 9 8 1 10-2c1.5-2 1.5-6 0-9s-3.5-5-7.5-5-3 0-4 1z" />
        <circle cx="8" cy="10" r="0.5" fill="currentColor" />
        <circle cx="15" cy="8" r="0.5" fill="currentColor" />
    </svg>
);

// ── Helpers ────────────────────────────────────────────────────────────────
function unwrap(response: any): any {
    if (!response) return null;
    if (typeof response === "object" && "success" in response) {
        if (!response.success) { console.error("[PD] err:", response.error); return null; }
        return response.result ?? null;
    }
    return response;
}

function fmt(price: any, currency: string | null | undefined): string {
    if (price == null || price === "") return "";
    const n = parseFloat(price);
    if (isNaN(n)) return "";
    return `${n.toFixed(2)} ${(currency || "").toUpperCase()}`;
}

function translateSyncStatus(status: string, detail: string, lang: string): string {
    if (status === "ok" || !status) return "";
    if (status === "steam_id_missing") return t("msg_sync_steam_id", lang);
    if (status === "wishlist_empty_or_private") return t("msg_sync_empty", lang);
    if (status === "wishlist_private") return t("msg_sync_private", lang);
    if (status === "wishlist_rate_limited") return t("msg_sync_limit", lang);
    if (detail === "parse_error") return t("msg_sync_html", lang);
    if (status === "wishlist_network_error") return t("msg_sync_net", lang);
    return `⚠️ ${status}`;
}


// ── URL opener (shared by GameCard and Sale card) ─────────────────────────
const sanitizeStoreUrl = (url: string) => {
    const raw = String(url || "").trim();
    if (!raw.startsWith("https://store.steampowered.com/")) {
        return "https://store.steampowered.com/";
    }
    return raw;
};

const openSaleUrl = (url: string) => {
    const safeUrl = sanitizeStoreUrl(url);
    try {
        // Try Decky/Steam Deck native method first
        const sc = (window as any).SteamClient;
        if (sc?.System?.OpenInSystemBrowser) {
            sc.System.OpenInSystemBrowser(safeUrl);
            return;
        }
        // Try Steam internal navigation for store URLs
        if (safeUrl.includes('store.steampowered.com')) {
            const nav = (window as any).SteamUIStore?.WindowStore?.SteamUIWindows?.[0];
            if (nav?.NavigateToSteamURL) {
                nav.NavigateToSteamURL(safeUrl);
                return;
            }
        }
        // Fallback: try Navigation API
        const nav = (window as any).Navigation;
        if (nav?.NavigateToExternalWeb) {
            nav.NavigateToExternalWeb(safeUrl);
            return;
        }
        // Last resort: window.open
        window.open(safeUrl, "_blank");
    } catch (e) {
        console.error("[PD] openSaleUrl:", e);
        window.open(safeUrl, "_blank");
    }
};

// ── Game card ──────────────────────────────────────────────────────────────
function GameCard({ game, lang }: { game: any; lang: string }) {
    const [imgTry, setImgTry] = useState(0);
    const imgUrls = [
        `https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/${game.appid}/library_600x900_2x.jpg`,
        game.capsule_url,
        `https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/${game.appid}/capsule_616x353.jpg`,
        `https://shared.fastly.steamstatic.com/store_item_assets/steam/apps/${game.appid}/header.jpg`,
        `https://cdn.cloudflare.steamstatic.com/steam/apps/${game.appid}/capsule_231x87.jpg`,
        `https://cdn.cloudflare.steamstatic.com/steam/apps/${game.appid}/header.jpg`,
        `https://cdn.cloudflare.steamstatic.com/steam/apps/${game.appid}/library_600x900_2x.jpg`,
        `https://shared.akamai.steamstatic.com/store_item_assets/steam/apps/${game.appid}/header.jpg`,
        `https://steamcdn-a.akamaihd.net/steam/apps/${game.appid}/header.jpg`,
        `https://steamcdn-a.akamaihd.net/steam/apps/${game.appid}/capsule_616x353.jpg`
    ].filter(Boolean);

    const discount = parseInt(game.discount_percent, 10) || 0;
    const currency = game.converted_currency || game.current_currency || game.currency || "";
    const currentPriceRaw = game.converted_price ?? game.price ?? game.current_price;
    const initialPriceRaw = game.converted_initial_price ?? game.initial_price ?? currentPriceRaw;
    const lowRaw = game.all_time_low_converted ?? game.all_time_low?.price ?? game.all_time_low_price;

    const isReleased = game.is_released !== false;
    const currentPriceNum = parseFloat(currentPriceRaw);
    const initialPriceNum = parseFloat(initialPriceRaw);
    const historicLowNum = parseFloat(lowRaw);

    const priceDataInconsistent = game.price_data_consistent === false || !!game.price_data_error;

    const getMetacriticStyle = (score: number) => {
        if (score >= 75) return { bg: '#66cc33', color: '#000' };
        if (score >= 50) return { bg: '#ffcc33', color: '#000' };
        return { bg: '#ff3333', color: '#fff' };
    };

    const hasPrice = currentPriceRaw != null && currentPriceRaw !== "" && !isNaN(currentPriceNum);
    const hasInitial = initialPriceRaw != null && initialPriceRaw !== "" && !isNaN(initialPriceNum);

    const releaseDateLabel = (() => {
        if (!game.release_date) return t("lbl_release_tba", lang);
        const ts = Date.parse(String(game.release_date));
        if (Number.isNaN(ts)) return String(game.release_date);
        return new Date(ts).toLocaleDateString(lang === "ru" ? "ru-RU" : "en-GB", { day: "numeric", month: "long", year: "numeric" });
    })();

    const currentPriceLabel = hasPrice ? fmt(currentPriceNum, currency) : "—";
    const stdPriceLabel = hasInitial ? fmt(initialPriceNum, currency) : "—";
    const storeUrl = sanitizeStoreUrl(game.store_url || `https://store.steampowered.com/app/${game.appid}/`);

    return (
        <Focusable style={{
            backgroundColor: "transparent",
            borderRadius: "8px",
            overflow: "hidden",
            border: "1px solid rgba(255,255,255,0.1)",
            display: "flex",
            flexDirection: "column" as any,
            marginBottom: "16px",
            boxShadow: "0 25px 50px -12px rgba(0, 0, 0, 0.25)",
            transition: "all 150ms cubic-bezier(0.4, 0, 0.2, 1)",
            cursor: "pointer"
        }}
            onActivate={() => openSaleUrl(storeUrl)}
            onClick={() => openSaleUrl(storeUrl)}
        >
            {/* Top Header */}
            <div style={{
                backgroundColor: "rgba(0,0,0,0.3)",
                padding: "8px 12px",
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                borderBottom: "1px solid rgba(255,255,255,0.05)"
            }}>
                <h3 style={{
                    fontWeight: "bold",
                    fontSize: "13px",
                    color: "#fff",
                    whiteSpace: "nowrap",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    maxWidth: "70%",
                    textTransform: "uppercase",
                    letterSpacing: "-0.025em",
                    margin: 0
                }}>{game.title}</h3>

                {game.metacritic_score ? (
                    <div style={{
                        display: "flex",
                        alignItems: "center",
                        gap: "6px",
                        backgroundColor: "rgba(0,0,0,0.4)",
                        padding: "2px 6px",
                        borderRadius: "4px",
                        border: "1px solid rgba(255,255,255,0.1)",
                        boxShadow: "0 1px 2px 0 rgba(0, 0, 0, 0.05)"
                    }}>
                        <span style={{ fontSize: "8px", fontWeight: 900, color: "#64748b", letterSpacing: "-0.05em", textTransform: "uppercase" }}>{t("lbl_metacritic", lang)}</span>
                        <div style={{
                            fontSize: "10px",
                            fontWeight: 900,
                            padding: "0 6px",
                            borderRadius: "2px",
                            minWidth: "22px",
                            textAlign: "center",
                            boxShadow: "0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)",
                            backgroundColor: getMetacriticStyle(game.metacritic_score).bg,
                            color: getMetacriticStyle(game.metacritic_score).color
                        }}>
                            {game.metacritic_score}
                        </div>
                    </div>
                ) : (
                    <div style={{
                        display: "flex",
                        alignItems: "center",
                        gap: "6px",
                        backgroundColor: "rgba(0,0,0,0.35)",
                        padding: "2px 6px",
                        borderRadius: "4px",
                        border: "1px solid rgba(255,255,255,0.08)"
                    }}>
                        <span style={{ fontSize: "8px", fontWeight: 900, color: "#64748b", letterSpacing: "-0.05em", textTransform: "uppercase" }}>{t("lbl_metacritic", lang)}</span>
                        <div style={{
                            fontSize: "10px",
                            fontWeight: 900,
                            padding: "0 6px",
                            borderRadius: "2px",
                            minWidth: "22px",
                            textAlign: "center",
                            backgroundColor: "#334155",
                            color: "#cbd5e1"
                        }}>
                            --
                        </div>
                    </div>
                )}
            </div>

            <div style={{ display: "flex", minHeight: "146px" }}>
                {/* Left Side: Cover Art */}
                <div style={{ width: "35%", flexShrink: 0, position: "relative", padding: "8px" }}>
                    <div style={{
                        width: "100%", height: "100%", overflow: "hidden", borderRadius: "6px",
                        boxShadow: "0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05)",
                        border: "1px solid rgba(255,255,255,0.05)", position: "relative"
                    }}>
                        {imgTry < imgUrls.length ? (
                            <img
                                src={imgUrls[imgTry]}
                                alt={game.title}
                                style={{ width: "100%", height: "100%", objectFit: "cover", display: "block" }}
                                onError={() => setImgTry(prev => prev + 1)}
                            />
                        ) : (
                            <div style={{
                                width: "100%", height: "100%", backgroundColor: "#2a2b2c",
                                display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
                                textAlign: "center", padding: "6px"
                            }}>
                                <span style={{ fontSize: "18px", marginBottom: "4px" }}>🎮</span>
                                <span style={{ fontSize: "9px", color: "#6b7280", padding: "0 4px", textAlign: "center", wordBreak: "break-word" as any }}>{game.title}</span>
                            </div>
                        )}

                        {discount > 0 && !priceDataInconsistent && (
                            <div style={{
                                position: "absolute", bottom: "6px", left: "50%", transform: "translateX(-50%)",
                                backgroundColor: "#4c6b22", color: "#beee11",
                                fontSize: "11px", fontWeight: 900, padding: "2px 6px",
                                borderRadius: "4px", boxShadow: "0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04)",
                                border: "1px solid rgba(190,238,17,0.2)"
                            }}>
                                -{discount}%
                            </div>
                        )}
                    </div>
                </div>

                {/* Right Side: Data Panel */}
                <div style={{ flex: 1, display: "flex", flexDirection: "column", padding: "10px 14px 10px 12px", justifyContent: "center", minWidth: 0, gap: "4px" }}>

                    <div style={{ display: "flex", flexDirection: "column", gap: "1px", width: "100%", overflow: "visible" }}>
                        <div style={{ fontSize: "9px", color: "#64748b", textTransform: "uppercase", fontWeight: 800, whiteSpace: "normal" }}>{t("lbl_current_price", lang)}</div>
                        <div style={{ fontSize: "11px", fontWeight: "normal", color: "#beee11", whiteSpace: "nowrap" }}>{currentPriceLabel}</div>
                    </div>

                    <div style={{ display: "flex", flexDirection: "column", gap: "1px", width: "100%", overflow: "visible" }}>
                        <div style={{ fontSize: "9px", color: "#64748b", textTransform: "uppercase", fontWeight: 800, whiteSpace: "normal" }}>{t("lbl_standard_price", lang)}</div>
                        <div style={{ fontSize: "11px", fontWeight: "normal", color: "#cbd5e1", whiteSpace: "nowrap" }}>{stdPriceLabel}</div>
                    </div>

                    <div style={{ display: "flex", flexDirection: "column", gap: "1px", width: "100%", overflow: "visible" }}>
                        <div style={{ fontSize: "8px", color: "#64748b", textTransform: "uppercase", fontWeight: 800, whiteSpace: "normal" }}>{t("lbl_release_date", lang)}</div>
                        <div style={{ fontSize: "9px", fontWeight: "normal", color: "#cbd5e1", whiteSpace: "nowrap" }}>{releaseDateLabel}</div>
                    </div>

                </div>
            </div>
        </Focusable>
    );
}

// ── Horizontal Toggle Row ──────────────────────────────────────────────────
const OPT_ON: React.CSSProperties = {
    padding: "8px 12px", background: "#66c0f4", color: "#171a21",
    borderRadius: "6px", fontSize: "11px", fontWeight: 900,
    cursor: "pointer", flex: "0 0 auto", textTransform: "uppercase",
    letterSpacing: "0.05em",
    boxShadow: "0 4px 6px -1px rgba(102,192,244,0.2)"
};
const OPT_OFF: React.CSSProperties = {
    padding: "8px 12px", background: "#171a21", color: "#fff",
    border: "1px solid rgba(255,255,255,0.1)",
    borderRadius: "6px", fontSize: "11px", fontWeight: "normal",
    cursor: "pointer", flex: "0 0 auto",
};

function OptionRow({ label, options, value, onChange }: {
    label: string;
    options: { label: string; value: string }[];
    value: string;
    onChange: (v: string) => void;
}) {
    return (
        <div style={{ marginBottom: "12px", width: "100%" }}>
            <div style={{ fontSize: "10px", fontWeight: 900, color: "#64748b", marginBottom: "8px", textTransform: "uppercase", letterSpacing: "0.2em" }}>{label}</div>
            <Focusable style={{ display: "flex", flexWrap: "wrap", gap: "6px" }} flow-children="horizontal">
                {options.map(o => (
                    <Focusable key={o.value} style={value === o.value ? OPT_ON : OPT_OFF}
                        onActivate={() => onChange(o.value)} onClick={() => onChange(o.value)}>
                        {o.label}
                    </Focusable>
                ))}
            </Focusable>
        </div>
    );
}

// ── Application State & Logic ──────────────────────────────────────────────
const TABS = ["wishlist", "sales", "settings"];
const TAB_KEYS: Record<string, string> = { wishlist: "tab_wishlist", sales: "tab_sales", settings: "tab_settings" };

const TAB_ON: React.CSSProperties = {
    flex: 1, textAlign: "center", padding: "10px 0",
    background: "#66c0f4", color: "#171a21",
    borderRadius: "6px", cursor: "pointer",
    display: "flex", justifyContent: "center", alignItems: "center",
    boxShadow: "0 4px 6px -1px rgba(102,192,244,0.3)"
};
const TAB_OFF: React.CSSProperties = {
    flex: 1, textAlign: "center", padding: "10px 0",
    background: "transparent", color: "#64748b",
    borderRadius: "6px", cursor: "pointer",
    display: "flex", justifyContent: "center", alignItems: "center"
};

const CURRENCY_OPTS = [
    { label: "USD", value: "USD" }, { label: "EUR", value: "EUR" },
    { label: "RUB", value: "RUB" }, { label: "KZT", value: "KZT" },
    { label: "UZS", value: "UZS" }, { label: "TRY", value: "TRY" },
    { label: "UAH", value: "UAH" }, { label: "BRL", value: "BRL" },
    { label: "CNY", value: "CNY" }, { label: "GBP", value: "GBP" },
];

const LANGUAGE_OPTS = [
    { label: "Рус", value: "ru" }, { label: "Eng", value: "en" }, { label: "中文", value: "zh-CN" },
];

const REGION_OPTS = [
    { label: "US", value: "us" }, { label: "EU", value: "eu" }, { label: "GB", value: "gb" },
    { label: "RU", value: "ru" }, { label: "KZ", value: "kz" }, { label: "UZ", value: "uz" },
    { label: "TR", value: "tr" }, { label: "AR", value: "ar" }, { label: "UA", value: "ua" },
    { label: "BR", value: "br" }, { label: "CN", value: "cn" }, { label: "IN", value: "in" },
];

const SALES_FALLBACK = [
    { name: "Steam Spring Sale", description: "Fallback calendar", url: "https://store.steampowered.com/sale/spring", startTs: 0, endTs: 0, status: "unknown", major: true },
    { name: "Steam Summer Sale", description: "Fallback calendar", url: "https://store.steampowered.com/sale/summer", startTs: 0, endTs: 0, status: "unknown", major: true },
    { name: "Steam Autumn Sale", description: "Fallback calendar", url: "https://store.steampowered.com/sale/autumn", startTs: 0, endTs: 0, status: "unknown", major: true },
    { name: "Steam Winter Sale", description: "Fallback calendar", url: "https://store.steampowered.com/sale/winter", startTs: 0, endTs: 0, status: "unknown", major: true },
];

const KNOWN_SALE_NAMES: Record<string, Record<string, string>> = {
    "ru": {
        "horses": "Фестиваль игр про лошадей",
        "steam horses sale": "Фестиваль игр про лошадей",
        "horse fest": "Фестиваль игр про лошадей",
        "steam spring sale": "Весенняя распродажа Steam",
        "steam summer sale": "Летняя распродажа Steam",
        "steam autumn sale": "Осенняя распродажа Steam",
        "steam winter sale": "Зимняя распродажа Steam",
        "quebec games celebration": "Праздник игр из Квебека",
    },
    "en": {
        "horses": "Horses Fest",
        "steam horses sale": "Steam Horses Sale",
        "horse fest": "Horse Fest",
        "steam spring sale": "Steam Spring Sale",
        "steam summer sale": "Steam Summer Sale",
        "steam autumn sale": "Steam Autumn Sale",
        "steam winter sale": "Steam Winter Sale",
        "quebec games celebration": "Quebec Games Celebration",
    },
    "zh-CN": {
        "horses": "马匹游戏节",
        "steam horses sale": "Steam 马匹特卖",
        "horse fest": "马匹游戏节",
        "steam spring sale": "Steam 春季特卖",
        "steam summer sale": "Steam 夏季特卖",
        "steam autumn sale": "Steam 秋季特卖",
        "steam winter sale": "Steam 冬季特卖",
        "quebec games celebration": "魁北克游戏节",
    }
};

function normalizeSalesPayload(rawSales: any, lang: string): { events: any[]; warning: string; source: string; fetchedAt: string } {
    const inputEvents = Array.isArray(rawSales?.events) ? rawSales.events : [];
    const normalized = inputEvents
        .map((ev: any) => {
            const startTs = Number(ev?.start_ts || 0);
            const endTs = Number(ev?.end_ts || 0);
            const safeUrl = sanitizeStoreUrl(ev?.url || "");
            const origName = String(ev?.name || "Steam Sale Event");
            const lowerName = origName.toLowerCase();
            const locDict = KNOWN_SALE_NAMES[lang] || KNOWN_SALE_NAMES["en"];
            const finalName = locDict[lowerName] || origName;

            return {
                name: finalName,
                description: String(ev?.description || ""),
                url: safeUrl,
                startTs: Number.isFinite(startTs) ? startTs : 0,
                endTs: Number.isFinite(endTs) ? endTs : 0,
                status: String(ev?.status || "unknown"),
                major: !!ev?.major,
                source: String(ev?.source || ""),
                urlValid: ev?.url_valid !== false,
            };
        })
        .filter((ev: any) => ev.url !== "https://store.steampowered.com/");

    return {
        events: normalized.length > 0 ? normalized : SALES_FALLBACK,
        warning: String(rawSales?.warning || ""),
        source: String(rawSales?.source || ""),
        fetchedAt: String(rawSales?.fetched_at || ""),
    };
}



const PotatoDeals = () => {
    const [activeTab, setActiveTab] = useState("wishlist");
    const [steamId, setSteamId] = useState("");
    const [currency, setCurrency] = useState("USD");
    const [language, setLanguage] = useState("ru");
    const [region, setRegion] = useState("us");
    const [games, setGames] = useState<any[]>([]);
    const [isLoading, setLoading] = useState(false);
    const [statusMsg, setStatusMsg] = useState("");
    const [saveStatus, setSaveStatus] = useState<"idle" | "saving" | "done">("idle");
    const [sortStrategy, setSortStrategy] = useState("discount");
    const [isSortOpen, setSortOpen] = useState(false);
    const [lastSync, setLastSync] = useState("");
    const [salesEvents, setSalesEvents] = useState<any[]>(SALES_FALLBACK);
    const [salesWarning, setSalesWarning] = useState("");
    const [salesSource, setSalesSource] = useState("");
    const [salesFetchedAt, setSalesFetchedAt] = useState("");
    const [salesLoading, setSalesLoading] = useState(false);

    const sortedGames = useMemo(() => {
        const arr = [...games];
        const pickPrice = (g: any) => {
            const raw = g.converted_price ?? g.current_price;
            const n = parseFloat(raw);
            return Number.isFinite(n) ? n : null;
        };
        if (sortStrategy === "discount") return arr.sort((a, b) => (b.discount_percent || 0) - (a.discount_percent || 0));
        if (sortStrategy === "price_asc") return arr.sort((a, b) => {
            const pa = pickPrice(a);
            const pb = pickPrice(b);
            if (pa == null && pb == null) return 0;
            if (pa == null) return 1;
            if (pb == null) return -1;
            return pa - pb;
        });
        if (sortStrategy === "price_desc") return arr.sort((a, b) => {
            const pa = pickPrice(a);
            const pb = pickPrice(b);
            if (pa == null && pb == null) return 0;
            if (pa == null) return 1;
            if (pb == null) return -1;
            return pb - pa;
        });
        if (sortStrategy === "title_asc") return arr.sort((a, b) => (a.title || "").localeCompare(b.title || ""));
        if (sortStrategy === "date_added") return arr.sort((a, b) => (a.wishlist_order || 0) - (b.wishlist_order || 0));
        if (sortStrategy === "metacritic") return arr.sort((a, b) => (b.metacritic_score || 0) - (a.metacritic_score || 0));
        return arr;
    }, [games, sortStrategy]);

    const saveSettings = useCallback(async (patch: Record<string, any>) => {
        try {
            await (call as any)("save_settings", { patch });
        } catch (e) {
            console.error("[PD] save_settings err:", e);
        }
    }, []);

    const refreshSales = useCallback(async () => {
        setSalesLoading(true);
        try {
            const raw = await (call as any)("get_sales_events");
            const data = unwrap(raw);
            const normalized = normalizeSalesPayload(data, language);
            setSalesEvents(normalized.events);
            setSalesWarning(normalized.warning);
            setSalesSource(normalized.source);
            setSalesFetchedAt(normalized.fetchedAt);
        } catch (e) {
            console.error("[PD] get_sales_events err:", e);
            setSalesEvents(SALES_FALLBACK);
            setSalesWarning("Live Steam sales feed unavailable. Showing fallback schedule.");
            setSalesSource("fallback_calendar");
            setSalesFetchedAt("");
        }
        setSalesLoading(false);
    }, [language]);

    useEffect(() => {
        let cancelled = false;
        const init = async () => {
            try {
                const raw = await (call as any)("get_bootstrap");
                if (cancelled) return;
                const data = unwrap(raw);
                if (!data) return;

                if (data.meta && data.meta.wishlist_last_sync) setLastSync(data.meta.wishlist_last_sync);

                const s = data.settings || {};

                if (s.steam_id) setSteamId(String(s.steam_id));
                if (s.currency) setCurrency(String(s.currency));
                if (s.language) setLanguage(String(s.language));
                if (s.region) setRegion(String(s.region));

                const normalizedSales = normalizeSalesPayload(data.sales || {}, s.language || "ru");
                setSalesEvents(normalizedSales.events);
                setSalesWarning(normalizedSales.warning);
                setSalesSource(normalizedSales.source);
                setSalesFetchedAt(normalizedSales.fetchedAt);

                if (Array.isArray(data.games) && data.games.length > 0) {
                    setGames(data.games);
                    setStatusMsg(`${t("msg_cache", s.language || "ru")} ${data.games.length}`);
                } else {
                    setStatusMsg(s.steam_id ? t("msg_click_refresh", s.language || "ru") : t("msg_enter_id", s.language || "ru"));
                }
            } catch (e) {
                console.error("[PD] init err", e);
            }
        };
        init();
        return () => { cancelled = true; };
    }, []);

    useEffect(() => {
        if (activeTab === "sales") {
            refreshSales();
        }
    }, [activeTab, refreshSales]);

    const fetchWishlist = async () => {
        setLoading(true);
        setStatusMsg(t("btn_loading", language));
        const sid = steamId.trim();
        try {
            if (sid) await saveSettings({ steam_id: sid });
            const raw = await (call as any)("sync_wishlist", { steam_id: sid || undefined });
            const data = unwrap(raw);
            if (!data) { setStatusMsg("⚠️ Backend Error"); setLoading(false); return; }

            if (data.meta && data.meta.wishlist_last_sync) setLastSync(data.meta.wishlist_last_sync);

            if (Array.isArray(data.games) && data.games.length > 0) {
                setGames(data.games);
            }
            const txt = translateSyncStatus(data.sync_status || "", data.sync_status_detail || "", language);
            setStatusMsg(txt || `${t("msg_loaded", language)} ${data.games?.length ?? 0}`);
        } catch (e) {
            console.error("[PD] sync", e);
            setStatusMsg(t("msg_sync_net", language));
        }
        setLoading(false);
    };

    const handleSaveSteamId = async () => {
        setSaveStatus("saving");
        try {
            const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error("timeout")), 10000));
            await Promise.race([saveSettings({ steam_id: steamId.trim() }), timeout]);
            setSaveStatus("done");
            setTimeout(() => setSaveStatus("idle"), 2500);
            // Auto-sync wishlist after successful save
            fetchWishlist();
        } catch (e: any) {
            console.error("[PD] save steam id:", e);
            setSaveStatus("idle");
            setStatusMsg(e?.message === "timeout" ? t("msg_save_timeout", language) : t("msg_save_error", language));
        }
    };

    const SVG_WISHLIST = <svg width="14" height="14" fill="currentColor" viewBox="0 0 20 20"><path d="M7 3a1 1 0 000 2h6a1 1 0 100-2H7zM4 7a1 1 0 011-1h10a1 1 0 110 2H5a1 1 0 01-1-1zM2 11a2 2 0 012-2h12a2 2 0 012 2v4a2 2 0 01-2 2H4a2 2 0 01-2-2v-4z" /></svg>;
    const SVG_SALES = <svg width="14" height="14" fill="currentColor" viewBox="0 0 20 20"><path d="M6 2a1 1 0 00-1 1v1H4a2 2 0 00-2 2v10a2 2 0 002 2h12a2 2 0 002-2V6a2 2 0 00-2-2h-1V3a1 1 0 10-2 0v1H7V3a1 1 0 00-1-1zm0 5a1 1 0 000 2h8a1 1 0 100-2H6z" /></svg>;
    const SVG_SETTINGS = <svg width="14" height="14" fill="currentColor" viewBox="0 0 20 20"><path fillRule="evenodd" d="M11.49 3.17c-.38-1.56-2.6-1.56-2.98 0a1.532 1.532 0 01-2.286.948c-1.372-.836-2.942.734-2.106 2.106.54.886.061 2.042-.947 2.287-1.561.379-1.561 2.6 0 2.978a1.532 1.532 0 01.947 2.287c-.836 1.372.734 2.942 2.106 2.106a1.532 1.532 0 012.287.947c.379 1.561 2.6 1.561 2.978 0a1.533 1.533 0 012.287-.947c1.372.836 2.942-.734 2.106-2.106a1.533 1.533 0 01.947-2.287c1.561-.379 1.561-2.6 0-2.978a1.532 1.532 0 01-.947-2.287c.836-1.372-.734-2.942-2.106-2.106a1.532 1.532 0 01-2.287-.947zM10 13a3 3 0 100-6 3 3 0 000 6z" clipRule="evenodd" /></svg>;

    const tabBar = (
        <div style={{ backgroundColor: "#1b2838", padding: "4px", borderRadius: "8px", margin: "12px 0 16px 0", border: "1px solid rgba(255,255,255,0.05)", boxShadow: "inset 0 2px 4px 0 rgba(0, 0, 0, 0.06)" }}>
            <Focusable style={{ display: "flex", gap: "4px" }} flow-children="horizontal">
                {TABS.map(tKey => {
                    const icon = tKey === "wishlist" ? SVG_WISHLIST : tKey === "sales" ? SVG_SALES : SVG_SETTINGS;
                    return (
                        <Focusable key={tKey} style={activeTab === tKey ? TAB_ON : TAB_OFF}
                            onActivate={() => setActiveTab(tKey)} onClick={() => setActiveTab(tKey)}>
                            {icon}
                        </Focusable>
                    );
                })}
            </Focusable>
        </div>
    );

    const wishlistContent = (
        <PanelSection>
            <PanelSectionRow>
                <Focusable
                    style={{
                        background: "#66c0f4", color: "#171a21",
                        borderRadius: "6px", padding: "10px 0",
                        textAlign: "center", cursor: "pointer",
                        fontWeight: 900, fontSize: "12px", textTransform: "uppercase",
                        letterSpacing: "0.05em",
                        boxShadow: "0 4px 6px -1px rgba(102,192,244,0.3)",
                        opacity: isLoading ? 0.6 : 1,
                        transition: "opacity 150ms ease",
                        display: "flex", alignItems: "center", justifyContent: "center", gap: "6px"
                    }}
                    onActivate={fetchWishlist}
                    onClick={fetchWishlist}
                >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
                        <path d="M21 2v6h-6" />
                        <path d="M3 12a9 9 0 0 1 15-6.7L21 8" />
                        <path d="M3 22v-6h6" />
                        <path d="M21 12a9 9 0 0 1-15 6.7L3 16" />
                    </svg>
                    {isLoading ? t("btn_loading", language) : t("btn_refresh", language)}
                </Focusable>
                {isLoading && (
                    <div style={{ textAlign: "center", fontSize: "10px", color: "#94a3b8", marginTop: "6px", fontStyle: "italic" }}>
                        {t("txt_loading_notice", language)}
                    </div>
                )}
            </PanelSectionRow>
            {games.length > 0 && (
                <PanelSectionRow>
                    <div style={{ position: "relative", display: "flex", flexDirection: "column", flex: 1 }}>
                        <Focusable
                            style={{
                                flex: 1, background: "#1b2838", color: "#fff",
                                border: "1px solid rgba(255,255,255,0.1)",
                                borderRadius: isSortOpen ? "6px 6px 0 0" : "6px",
                                padding: "10px 0", margin: 0,
                                textAlign: "center", cursor: "pointer",
                                fontWeight: 900, fontSize: "12px", textTransform: "uppercase",
                                letterSpacing: "0.05em",
                                boxShadow: "0 4px 6px -1px rgba(0,0,0,0.3)",
                                display: "flex", alignItems: "center", justifyContent: "center", gap: "6px",
                                transition: "all 0.1s ease-in-out"
                            }}
                            onActivate={() => setSortOpen(!isSortOpen)}
                            onClick={() => setSortOpen(!isSortOpen)}
                        >
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
                                <path d="m3 16 4 4 4-4" />
                                <path d="M7 20V4" />
                                <path d="M11 4h10" />
                                <path d="M11 8h7" />
                                <path d="M11 12h4" />
                            </svg>
                            {t("lbl_sort_by", language)}
                            <span style={{ fontSize: "10px", marginLeft: "2px", transform: isSortOpen ? "rotate(180deg)" : "rotate(0deg)", transition: "transform 0.2s" }}>▼</span>
                        </Focusable>
                        {isSortOpen && (
                            <div style={{ position: "absolute", top: "100%", left: 0, right: 0, zIndex: 1000 }}>
                                <Focusable style={{
                                    width: "100%", background: "#1b2838",
                                    borderRadius: "0 0 6px 6px",
                                    border: "1px solid rgba(255,255,255,0.1)",
                                    borderTop: "none", overflow: "hidden",
                                    display: "flex", flexDirection: "column",
                                    boxShadow: "0 10px 15px -3px rgba(0,0,0,0.5)"
                                }} flow-children="vertical">
                                    {[
                                        { val: "discount", lbl: t("sort_discount", language) },
                                        { val: "price_asc", lbl: t("sort_price_asc", language) },
                                        { val: "price_desc", lbl: t("sort_price_desc", language) },
                                        { val: "title_asc", lbl: t("sort_title_asc", language) },
                                        { val: "date_added", lbl: t("sort_date_added", language) },
                                        { val: "metacritic", lbl: t("sort_metacritic", language) }
                                    ].map(o => (
                                        <Focusable
                                            key={o.val}
                                            style={{
                                                padding: "10px 16px",
                                                cursor: "pointer",
                                                fontSize: "12px",
                                                fontWeight: sortStrategy === o.val ? 900 : 500,
                                                color: sortStrategy === o.val ? "#66c0f4" : "#cbd5e1",
                                                background: sortStrategy === o.val ? "rgba(255,255,255,0.05)" : "transparent",
                                                borderBottom: "1px solid rgba(255,255,255,0.05)"
                                            }}
                                            onActivate={() => { setSortStrategy(o.val); setSortOpen(false); }}
                                            onClick={() => { setSortStrategy(o.val); setSortOpen(false); }}
                                        >
                                            {sortStrategy === o.val ? `✓ ${o.lbl}` : o.lbl}
                                        </Focusable>
                                    ))}
                                </Focusable>
                            </div>
                        )}
                    </div>
                </PanelSectionRow>
            )}
            {statusMsg && (
                <div style={{ padding: "4px 8px 8px", color: "#6b7280", fontSize: "11px", textAlign: "center" }}>
                    {statusMsg}
                </div>
            )}
            {lastSync && (
                <div style={{ padding: "0 8px 8px", color: "#475569", fontSize: "9px", textAlign: "center", textTransform: "uppercase", letterSpacing: "0.1em" }}>
                    Sync: {new Date(lastSync).toLocaleString()}
                </div>
            )}
            {sortedGames.map(game => (
                <PanelSectionRow key={game.appid}>
                    <GameCard game={game} lang={language} />
                </PanelSectionRow>
            ))}
            {!isLoading && games.length === 0 && (
                <PanelSectionRow>
                    <div style={{ color: "#4b5563", fontSize: "12px", textAlign: "center", padding: "12px" }}>
                        {t("txt_no_games", language)}
                    </div>
                </PanelSectionRow>
            )}
        </PanelSection>
    );

    const salesContent = (
        <PanelSection>
            {salesWarning && (
                <PanelSectionRow>
                    <div style={{
                        color: "#fbbf24",
                        fontSize: "10px",
                        textAlign: "center",
                        padding: "6px 12px",
                        border: "1px solid rgba(251,191,36,0.25)",
                        borderRadius: "8px",
                        background: "rgba(251,191,36,0.08)"
                    }}>
                        {salesWarning}
                    </div>
                </PanelSectionRow>
            )}

            {salesEvents.length === 0 && (
                <PanelSectionRow>
                    <div style={{ color: "#6b7280", fontSize: "12px", textAlign: "center", padding: "12px" }}>
                        {t("txt_no_sales", language)}
                    </div>
                </PanelSectionRow>
            )}

            <div style={{ display: "flex", flexDirection: "column", gap: "10px", paddingBottom: "16px" }}>
                {salesEvents.map((ev, idx) => {
                    const now = new Date().getTime();
                    const sDate = Number(ev.startTs || 0) > 0 ? Number(ev.startTs) * 1000 : 0;
                    const eDate = Number(ev.endTs || 0) > 0 ? Number(ev.endTs) * 1000 : 0;
                    const hasWindow = sDate > 0 && eDate > sDate;

                    let progress = 0;
                    if (hasWindow) {
                        if (now > eDate) progress = 100;
                        else if (now > sDate) progress = Math.round(((now - sDate) / (eDate - sDate)) * 100);
                    }

                    const status = String(ev.status || "unknown");
                    const isLive = status === "active" || (hasWindow && progress > 0 && progress < 100);
                    const isFuture = status === "upcoming" || (hasWindow && progress === 0);
                    const dateRange = hasWindow
                        ? `${new Date(sDate).toLocaleDateString()} — ${new Date(eDate).toLocaleDateString()}`
                        : t("txt_dates_unconfirmed", language) || "Dates not confirmed";
                    const isConfirmed = status === "active" || status === "upcoming";
                    const isClickable = ev.urlValid && ev.url && ev.url !== "https://store.steampowered.com/";

                    return (
                        <PanelSectionRow key={idx}>
                            <Focusable
                                style={{
                                    padding: "12px", borderRadius: "8px",
                                    backgroundColor: isLive ? "rgba(102,192,244,0.1)" : "transparent",
                                    border: isLive ? "1px solid rgba(102,192,244,0.5)" : "1px solid rgba(255,255,255,0.05)",
                                    boxShadow: isLive ? "0 0 20px rgba(102,192,244,0.15)" : "none",
                                    transition: "all 0.3s ease",
                                    width: "100%", boxSizing: "border-box",
                                    cursor: isClickable ? "pointer" : "default",
                                    opacity: isClickable ? 1 : 0.7
                                }}
                                onActivate={() => isClickable && openSaleUrl(ev.url)}
                                onClick={() => isClickable && openSaleUrl(ev.url)}
                            >
                                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "8px" }}>
                                    <div style={{ flex: 1, minWidth: 0, paddingRight: '8px' }}>
                                        <h3 style={{
                                            fontWeight: 900, textTransform: "uppercase", letterSpacing: "-0.025em", lineHeight: 1, marginBottom: "4px",
                                            color: ev.major ? "#66c0f4" : "#fff", fontSize: ev.major ? "13px" : "11px",
                                            margin: "0 0 4px 0"
                                        }}>
                                            {ev.name}
                                        </h3>
                                        {ev.description && (
                                            <p style={{ fontSize: "10px", color: "rgba(148, 163, 184, 1)", fontWeight: 500, marginBottom: "8px", lineHeight: 1.25, margin: "0 0 8px 0" }}>
                                                {ev.description}
                                            </p>
                                        )}
                                        <span style={{ fontSize: "9px", color: "#64748b", fontWeight: 900, textTransform: "uppercase", letterSpacing: "0.1em" }}>
                                            {dateRange}
                                        </span>
                                    </div>
                                    <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: "4px", flexShrink: 0 }}>
                                        <span style={{
                                            fontSize: "8px", fontWeight: 900, padding: "2px 6px", borderRadius: "4px", textTransform: "uppercase",
                                            backgroundColor: isConfirmed ? "rgba(34, 197, 94, 0.2)" : "rgba(234, 179, 8, 0.2)",
                                            color: isConfirmed ? "#4ade80" : "#eab308"
                                        }}>
                                            {isConfirmed ? t("sales_confirmed", language) : t("sales_predicted", language)}
                                        </span>
                                        {isLive && <span style={{ fontSize: "9px", fontWeight: 900, color: "#beee11" }}>{t("sales_live_now", language)}</span>}
                                        {!isClickable && <span style={{ fontSize: "9px", fontWeight: 900, color: "#94a3b8" }}>{t("txt_page_not_avail", language) || "Page not available yet"}</span>}
                                        {isClickable && (
                                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="#64748b" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                                <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
                                                <polyline points="15 3 21 3 21 9"></polyline>
                                                <line x1="10" y1="14" x2="21" y2="3"></line>
                                            </svg>
                                        )}
                                    </div>
                                </div>

                                {hasWindow && (
                                    <>
                                        <div style={{ width: "100%", height: "4px", backgroundColor: "rgba(255,255,255,0.05)", borderRadius: "2px", overflow: "hidden", marginTop: "8px" }}>
                                            <div style={{ height: "100%", width: `${progress}%`, backgroundColor: isLive ? "#beee11" : "#64748b", transition: "width 0.5s ease" }}></div>
                                        </div>
                                        <div style={{ display: "flex", justifyContent: "space-between", fontSize: "8px", fontWeight: 900, textTransform: "uppercase", color: "#64748b", letterSpacing: "0.1em", marginTop: "4px" }}>
                                            <span>{t("sales_progress", language)}</span>
                                            <span style={{ color: "#66c0f4" }}>{progress}%</span>
                                        </div>
                                    </>
                                )}
                            </Focusable>
                        </PanelSectionRow>
                    );
                })}
            </div>
            {
                salesEvents.length > 0 && (
                    <div style={{ fontSize: "8px", color: "#475569", textAlign: "center", padding: "0 24px 24px", textTransform: "uppercase", fontWeight: "bold", letterSpacing: "0.2em", lineHeight: 1.625 }}>
                        {t("sales_disclaimer", language)}
                        {salesSource ? ` • ${salesSource}` : ""}
                        {salesFetchedAt ? ` • ${new Date(salesFetchedAt).toLocaleString()}` : ""}
                        {salesLoading ? " • ..." : ""}
                    </div>
                )
            }
        </PanelSection >
    );

    const settingsContent = (
        <PanelSection>
            <PanelSectionRow>
                <TextField
                    label="Steam ID"
                    description={t("desc_steamid", language)}
                    value={steamId}
                    onChange={(e: any) => setSteamId(typeof e === 'string' ? e : (e?.target?.value || ""))}
                />
            </PanelSectionRow>
            <PanelSectionRow>
                <Focusable
                    style={{
                        background: "#66c0f4", color: "#171a21",
                        borderRadius: "6px", padding: "10px 0",
                        textAlign: "center", cursor: "pointer",
                        fontWeight: 900, fontSize: "12px", textTransform: "uppercase",
                        letterSpacing: "0.05em",
                        boxShadow: "0 4px 6px -1px rgba(102,192,244,0.3)",
                        opacity: saveStatus === "saving" ? 0.6 : 1,
                        transition: "opacity 150ms ease",
                        display: "flex", alignItems: "center", justifyContent: "center", gap: "6px"
                    }}
                    onActivate={handleSaveSteamId}
                    onClick={handleSaveSteamId}
                >
                    {saveStatus === "done" ? (
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
                            <polyline points="20 6 9 17 4 12"></polyline>
                        </svg>
                    ) : (
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
                            <path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"></path>
                            <polyline points="17 21 17 13 7 13 7 21"></polyline>
                            <polyline points="7 3 7 8 15 8"></polyline>
                        </svg>
                    )}
                    {saveStatus === "saving" ? t("btn_saving", language) : saveStatus === "done" ? t("btn_saved", language) : t("btn_save_id", language)}
                </Focusable>
            </PanelSectionRow>

            <PanelSectionRow>
                <OptionRow label={t("lbl_currency", language)} options={CURRENCY_OPTS} value={currency}
                    onChange={v => { setCurrency(v); saveSettings({ currency: v }); }} />
            </PanelSectionRow>
            <PanelSectionRow>
                <OptionRow label={t("lbl_language", language)} options={LANGUAGE_OPTS} value={language}
                    onChange={v => { setLanguage(v); saveSettings({ language: v }); }} />
            </PanelSectionRow>
            <PanelSectionRow>
                <OptionRow label={t("lbl_region", language)} options={REGION_OPTS} value={region}
                    onChange={v => { setRegion(v); saveSettings({ region: v }); }} />
            </PanelSectionRow>

            <PanelSectionRow>
                <Focusable
                    style={{
                        background: "#66c0f4", color: "#171a21",
                        borderRadius: "6px", padding: "10px 0",
                        textAlign: "center", cursor: "pointer",
                        fontWeight: 900, fontSize: "12px", textTransform: "uppercase",
                        letterSpacing: "0.05em",
                        boxShadow: "0 4px 6px -1px rgba(102,192,244,0.3)",
                        transition: "opacity 150ms ease",
                        display: "flex", alignItems: "center", justifyContent: "center", gap: "6px"
                    }}
                    onActivate={async () => {
                        setStatusMsg(t("btn_loading", language));
                        try {
                            const raw = await (call as any)("update_currency_rates", { force: true });
                            const res = unwrap(raw);
                            if (res && res.rates_update && res.rates_update.error) {
                                setStatusMsg(t("msg_rates_err", language) + ": " + res.rates_update.error);
                            } else {
                                setStatusMsg(t("msg_rates_ok", language));
                            }
                        } catch {
                            setStatusMsg(t("msg_rates_err", language));
                        }
                    }}
                    onClick={async () => {
                        setStatusMsg(t("btn_loading", language));
                        try {
                            const raw = await (call as any)("update_currency_rates", { force: true });
                            const res = unwrap(raw);
                            if (res && res.rates_update && res.rates_update.error) {
                                setStatusMsg(t("msg_rates_err", language) + ": " + res.rates_update.error);
                            } else {
                                setStatusMsg(t("msg_rates_ok", language));
                            }
                        } catch {
                            setStatusMsg(t("msg_rates_err", language));
                        }
                    }}
                >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
                        <polyline points="23 4 23 10 17 10"></polyline>
                        <polyline points="1 20 1 14 7 14"></polyline>
                        <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
                    </svg>
                    {t("btn_update_rates", language)}
                </Focusable>
            </PanelSectionRow>

            <div style={{ padding: "8px", color: "#374151", fontSize: "10px", textAlign: "right" }}>
                {BUILD}
            </div>
        </PanelSection>
    );

    return (
        <>
            <PanelSection>
                {tabBar}
            </PanelSection>
            {activeTab === "wishlist" && wishlistContent}
            {activeTab === "sales" && salesContent}
            {activeTab === "settings" && settingsContent}
        </>
    );
};

// @ts-ignore
export default definePlugin(() => ({
    name: "Potato Deals",
    titleView: <div className={staticClasses.Title}>🥔 Potato Deals</div>,
    content: <PotatoDeals />,
    icon: <PotatoIcon />,
    onDismount() { },
}));
