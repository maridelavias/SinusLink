import os
import re
import asyncio
import tempfile
import zipfile
from io import BytesIO
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
    InputMediaPhoto,
    InputMediaDocument,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    BotCommand,
    CallbackQuery,
    InputFile,
)
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.error import TimedOut, BadRequest, RetryAfter
from telegram.request import HTTPXRequest

import app.db as db
from app.utils import log

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан или задан неверно в .env")

LOR_TARGET_CHAT_ID = int(os.getenv("LOR_TARGET_CHAT_ID", "0"))
MAX_ZIP_MB = int(os.getenv("MAX_ZIP_MB", "47"))

MAIN_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("🆕 Начать новую консультацию")],
        [KeyboardButton("✍️ Заполнить профиль")],
        [KeyboardButton("ℹ️ Мои данные")],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    is_persistent=True,
    input_field_placeholder="Выберите действие",
)

BTN_FILL_PROFILE_RE = re.compile(r"(?:✍️\ufe0f?\s*)?заполнить профиль$", re.IGNORECASE)
BTN_NEW_CONSULT_RE = re.compile(r"(?:🆕\ufe0f?\s*)?начать новую консультацию$", re.IGNORECASE)
BTN_MY_DATA_RE = re.compile(r"(?:ℹ️\ufe0f?\s*)?мои данные$", re.IGNORECASE)

STATE_COMPLAINTS, STATE_HISTORY, STATE_PLAN, STATE_FILES, STATE_CONFIRM = range(5)
STATE_REG_NAME, STATE_REG_PHONE, STATE_REG_WORK = range(10, 13)


def build_dentist_html(dentist: dict) -> str:
    name = dentist.get("full_name") or "—"
    username = dentist.get("tg_username")
    tg_id = dentist.get("tg_id")

    if username:
        return f'{name} (<a href="https://t.me/{username}">@{username}</a>)'
    elif tg_id:
        return f'{name} (<a href="tg://user?id={tg_id}">написать</a>)'
    else:
        return name


def build_summary_html(consult: dict, dentist: dict) -> str:
    return (
        "<b>Заявка для консультации ЛОР</b>\n"
        f"<b>Жалобы</b>: {consult.get('patient_complaints', '—')}\n"
        f"<b>Анамнез</b>: {consult.get('patient_history', '—')}\n"
        f"<b>Планируемая работа</b>: {consult.get('planned_work', '—')}\n\n"
        f"<b>Стоматолог</b>: {build_dentist_html(dentist)}\n"
        f"Тел.: {dentist.get('phone') or '—'}; "
        f"Место работы: {dentist.get('workplace') or '—'}"
    )


def html_to_plain(html_text: str) -> str:
    return (
        html_text.replace("<b>", "")
        .replace("</b>", "")
        .replace('<a href="', "")
        .replace('">', " ")
        .replace("</a>", "")
    )


def short_caption(html_text: str) -> str:
    CAPTION_LIMIT = 1024
    if len(html_text) <= CAPTION_LIMIT:
        return html_text
    cut = html_text[: CAPTION_LIMIT - 20]
    cut = cut.rsplit(" ", 1)[0]
    return cut + " … (полный текст в 00_summary.txt)"


def build_deeplink_keyboard(dentist: dict) -> Optional[InlineKeyboardMarkup]:
    username = dentist.get("tg_username")
    tg_id = dentist.get("tg_id")

    url = f"https://t.me/{username}" if username else (f"tg://user?id={tg_id}" if tg_id else None)
    if not url:
        return None

    return InlineKeyboardMarkup([[InlineKeyboardButton("💬 Написать стоматологу", url=url)]])


async def _send_as_media_groups_with_caption(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    caption_html: str,
    atts: List[dict],
    reply_markup: Optional[InlineKeyboardMarkup],
    dentist: dict,
):
    batch: List[InputMediaPhoto | InputMediaDocument] = []
    first_item_used = False

    async def flush():
        nonlocal batch
        if not batch:
            return
        to_send = batch[:10]
        del batch[:10]
        await context.bot.send_media_group(chat_id=chat_id, media=to_send)

    for a in atts:
        if a["file_type"] == "photo":
            if not first_item_used:
                batch.append(InputMediaPhoto(media=a["file_id"], caption=caption_html, parse_mode=ParseMode.HTML))
                first_item_used = True
            else:
                batch.append(InputMediaPhoto(media=a["file_id"]))
        else:
            if not first_item_used:
                batch.append(InputMediaDocument(media=a["file_id"], caption=caption_html, parse_mode=ParseMode.HTML))
                first_item_used = True
            else:
                batch.append(InputMediaDocument(media=a["file_id"]))

        if len(batch) == 10:
            await flush()

    if batch:
        await flush()

    if reply_markup:
        try:
            await context.bot.send_message(chat_id=chat_id, text="Связаться со стоматологом:", reply_markup=reply_markup)
        except BadRequest:
            link = f"https://t.me/{dentist.get('tg_username')}" if dentist.get("tg_username") else f"tg://user?id={dentist.get('tg_id')}"
            await context.bot.send_message(chat_id=chat_id, text=f"💬 Связаться со стоматологом: {link}", disable_web_page_preview=True)


async def _build_and_send_zip(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, consult: dict, dentist: dict, atts: List[dict]
):
    html_text = build_summary_html(consult, dentist)
    plain_text = html_to_plain(html_text)
    caption_text = short_caption(html_text)
    kb = build_deeplink_keyboard(dentist)

    total_size = 0
    files_meta: List[Tuple[dict, object]] = []
    for a in atts:
        f = await context.bot.get_file(a["file_id"])
        files_meta.append((a, f))
        if getattr(f, "file_size", None):
            total_size += f.file_size

    if total_size > MAX_ZIP_MB * 1024 * 1024:
        await _send_as_media_groups_with_caption(context, chat_id, caption_text, atts, kb, dentist)
        return

    with tempfile.TemporaryDirectory() as tmp:
        summary_path = os.path.join(tmp, "00_summary.txt")
        with open(summary_path, "w", encoding="utf-8") as out:
            out.write(plain_text + "\n")

        local_paths = [summary_path]
        for i, (a, fobj) in enumerate(files_meta, 1):
            ext = ".jpg" if a["file_type"] == "photo" else ".bin"
            out_path = os.path.join(tmp, f"attachment_{i}{ext}")
            await fobj.download_to_drive(out_path, read_timeout=120.0)
            local_paths.append(out_path)

        bio = BytesIO()
        with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as z:
            for p in local_paths:
                z.write(p, arcname=os.path.basename(p))
        bio.seek(0)

        try:
            await context.bot.send_document(
                chat_id=chat_id,
                document=InputFile(bio, filename="lor_consultation.zip"),
                caption=caption_text,
                parse_mode=ParseMode.HTML,
                read_timeout=120.0,
                reply_markup=kb,
                disable_content_type_detection=True,
            )
        except (TimedOut, BadRequest):
            await _send_as_media_groups_with_caption(context, chat_id, caption_text, atts, kb, dentist)



async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await db.upsert_dentist(user.id, tg_username=user.username)
    dentist = await db.get_dentist_by_tg_id(user.id)
    dentist.setdefault("tg_id", user.id)

    profile_empty = not (dentist.get("full_name") or dentist.get("phone") or dentist.get("workplace"))
    if profile_empty:
        text = (
            "Здравствуйте! Укажите, пожалуйста, жалобы, анамнез, планируемую Вашу работу, прикрепите КТ сканы в коронарной и сагитальной проекции 📑\n"
            "Похоже, Ваш профиль ещё не заполнен ✍🏼\nЗаполните, пожалуйста, данные о себе и начните новую консультацию ⬇️"
        )
    else:
        text = (
            "Здравствуйте! Укажите, пожалуйста, жалобы, анамнез, планируемую Вашу работу, прикрепите КТ сканы в коронарной и сагитальной проекции 📑\n"
            "Проверьте свои данные и начните новую консультацию ⬇️"
        )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=MAIN_KB)


async def cmd_me(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dentist = await db.get_dentist_by_tg_id(update.effective_user.id)
    username_line = f"Username: @{dentist['tg_username']}" if dentist.get("tg_username") else "Username: —"
    text = (
        "<b>Ваши данные:</b>\n"
        f"Имя: {dentist.get('full_name') or '—'}\n"
        f"Телефон: {dentist.get('phone') or '—'}\n"
        f"Место работы: {dentist.get('workplace') or '—'}\n"
        f"{username_line}"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=MAIN_KB)


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    items = await db.list_consultations_by_dentist(update.effective_user.id)
    if not items:
        await update.message.reply_text("У вас пока нет отправленных заявок.", reply_markup=MAIN_KB)
        return
    lines = []
    kb_rows = []
    for c in items[:20]:
        cid = c["id"]
        lines.append(f"#{cid} · {c['created_at']} · статус: {c.get('status', '—')}")
        kb_rows.append([InlineKeyboardButton(f"Открыть #{cid}", callback_data=f"view_consult:{cid}")])
    await update.message.reply_text("Последние заявки:\n" + "\n".join(lines), reply_markup=InlineKeyboardMarkup(kb_rows))


async def cb_view_consult(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query: CallbackQuery = update.callback_query
    await query.answer()
    try:
        cid = int(query.data.split(":")[1])
    except Exception:
        await query.edit_message_text("Некорректный ID.")
        return
    c = await db.get_consultation_by_id(cid)
    if not c:
        await query.edit_message_text("Заявка не найдена.")
        return
    txt = (
        f"<b>Заявка #{c['id']}</b>\n"
        f"Статус: {c.get('status','—')}\n"
        f"Создана: {c.get('created_at','—')}\n\n"
        "Детали анкеты сохраняются в черновике до отправки; архив содержит полный текст и файлы."
    )
    await query.edit_message_text(txt, parse_mode=ParseMode.HTML)



async def reg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🦷 Заполним профиль стоматолога\nВведите ФИО:", reply_markup=ReplyKeyboardRemove())
    return STATE_REG_NAME


async def reg_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["reg_full_name"] = update.message.text.strip()
    await update.message.reply_text("Введите телефон (в любом удобном формате):")
    return STATE_REG_PHONE


async def reg_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["reg_phone"] = update.message.text.strip()
    await update.message.reply_text("Введите место работы (клиника, город):")
    return STATE_REG_WORK


async def reg_work(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    full_name = context.user_data.pop("reg_full_name", "").strip()
    phone = context.user_data.pop("reg_phone", "").strip()
    workplace = update.message.text.strip()
    await db.upsert_dentist(user.id, full_name=full_name, phone=phone, workplace=workplace, tg_username=user.username)
    await update.message.reply_text("✅ Профиль сохранён.", reply_markup=MAIN_KB)
    return ConversationHandler.END


async def new_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    consult, atts = await db.load_draft(user.id)
    if consult and (consult.get("patient_complaints") or atts):
        context.user_data["consult"] = consult
        context.user_data["attachments"] = atts
        await update.message.reply_text(
            "У вас есть незавершённая консультация. Хотите продолжить?",
            reply_markup=ReplyKeyboardMarkup([["▶️ Продолжить", "🔄 Начать заново"]], resize_keyboard=True),
        )
        return STATE_CONFIRM

    context.user_data["consult"] = {}
    context.user_data["attachments"] = []
    await update.message.reply_text("1/4. Жалобы пациента:", reply_markup=ReplyKeyboardRemove())
    return STATE_COMPLAINTS


async def new_complaints(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["consult"]["patient_complaints"] = update.message.text.strip()
    await db.save_draft(update.effective_user.id, context.user_data["consult"], context.user_data["attachments"])
    await update.message.reply_text("2/4. Анамнез / сопутствующие данные (кратко):")
    return STATE_HISTORY


async def new_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["consult"]["patient_history"] = update.message.text.strip()
    await db.save_draft(update.effective_user.id, context.user_data["consult"], context.user_data["attachments"])
    await update.message.reply_text("3/4. Планируемая стоматологическая работа:")
    return STATE_PLAN


async def new_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["consult"]["planned_work"] = update.message.text.strip()
    await db.save_draft(update.effective_user.id, context.user_data["consult"], context.user_data["attachments"])
    await update.message.reply_text(
        "4/4. Прикрепите снимки/файлы (можно несколько, до 40 Мб). Когда закончите — нажмите «Готово».",
        reply_markup=ReplyKeyboardMarkup([["Готово"]], resize_keyboard=True),
    )
    return STATE_FILES


async def new_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message and update.message.photo:
        file_id = update.message.photo[-1].file_id
        context.user_data["attachments"].append({"file_id": file_id, "file_type": "photo"})
    elif update.message and update.message.document:
        doc = update.message.document
        context.user_data["attachments"].append({"file_id": doc.file_id, "file_type": "document"})
    await db.save_draft(update.effective_user.id, context.user_data["consult"], context.user_data["attachments"])
    await update.message.reply_text("Файл добавлен. Прикрепите ещё или нажмите «Готово».", reply_markup=ReplyKeyboardMarkup([["Готово"]], resize_keyboard=True))
    return STATE_FILES


async def new_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    consult = context.user_data["consult"]
    dentist = await db.get_dentist_by_tg_id(user.id)
    dentist.setdefault("tg_id", user.id)
    atts = context.user_data["attachments"]

    preview = build_summary_html(consult, dentist) + f"\n\n📎 Прикреплено файлов: {len(atts)}"
    await update.message.reply_text(
        preview,
        parse_mode=ParseMode.HTML,
        reply_markup=ReplyKeyboardMarkup([["✅ Отправить", "❌ Отмена"], ["🔄 Начать заново"]], resize_keyboard=True),
    )
    return STATE_CONFIRM


async def new_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    choice = update.message.text
    user = update.effective_user
    consult = context.user_data.get("consult", {})
    dentist = await db.get_dentist_by_tg_id(user.id)
    dentist.setdefault("tg_id", user.id)
    atts = context.user_data.get("attachments", [])

    if choice.startswith("✅"):
        await _build_and_send_zip(context, LOR_TARGET_CHAT_ID, consult, dentist, atts)
        await db.insert_consultation_log(user.id, status="sent")
        await db.clear_draft(user.id)
        await update.message.reply_text("✅ Заявка отправлена ЛОР-врачу.", reply_markup=MAIN_KB)
        return ConversationHandler.END

    if choice.startswith("❌"):
        await db.clear_draft(user.id)
        await update.message.reply_text("❌ Отменено.", reply_markup=MAIN_KB)
        return ConversationHandler.END

    if choice.startswith("🔄"):
        await db.clear_draft(user.id)
        context.user_data["consult"] = {}
        context.user_data["attachments"] = []
        await update.message.reply_text("Начинаем заново. 1/4 Жалобы пациента:", reply_markup=ReplyKeyboardRemove())
        return STATE_COMPLAINTS

    if choice.startswith("▶️"):
        await update.message.reply_text("Продолжаем заполнение.", reply_markup=ReplyKeyboardRemove())
        if not consult.get("patient_history"):
            await update.message.reply_text("2/4. Анамнез / сопутствующие данные (кратко):")
            return STATE_HISTORY
        if not consult.get("planned_work"):
            await update.message.reply_text("3/4. Планируемая стоматологическая работа:")
            return STATE_PLAN
        await update.message.reply_text("4/4. Прикрепите снимки/файлы (можно несколько, до 40 Мб). Когда закончите — нажмите «Готово».")
        return STATE_FILES



async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.exception("Unhandled error", exc_info=context.error)
    try:
        if isinstance(update, Update) and update.effective_chat:
            await context.bot.send_message(update.effective_chat.id, "Произошла ошибка. Попробуйте ещё раз.")
    except Exception:
        pass


async def safe_post_init(application):
    async def safe_call(coro, label):
        try:
            return await coro
        except (BadRequest, RetryAfter) as e:
            log.warning(f"{label} skipped: {e}")

    await safe_call(
        application.bot.set_my_commands(
            [
                BotCommand("start", "Главное меню"),
                BotCommand("fill", "Заполнить профиль"),
                BotCommand("new", "Новая консультация"),
                BotCommand("me", "Мои данные"),
                BotCommand("list", "Список моих заявок"),
                BotCommand("set_name", "Изменить ФИО"),
                BotCommand("set_phone", "Изменить телефон"),
                BotCommand("set_workplace", "Изменить место работы"),
                BotCommand("cancel", "Отмена"),
            ]
        ),
        "set_my_commands",
    )

    await safe_call(
        application.bot.set_my_short_description("Бот для быстрой связи между стоматологом-хирургом и хирургом-отоларингологом для планирования совместного лечения пациента."),
        "set_my_short_description",
    )

    await safe_call(
        application.bot.set_my_description(
            "Помогает стоматологу быстро сформировать и отправить анкету пациента для консультации с ЛОР-врачом."
        ),
        "set_my_description",
    )


def build_application():
    request = HTTPXRequest(connect_timeout=10.0, read_timeout=120.0, write_timeout=120.0, pool_timeout=10.0)
    app = ApplicationBuilder().token(BOT_TOKEN).request(request).post_init(safe_post_init).build()

    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("me", cmd_me))
    app.add_handler(CommandHandler("list", cmd_list))

    reg_conv = ConversationHandler(
        entry_points=[CommandHandler("fill", reg_start), MessageHandler(filters.Regex(BTN_FILL_PROFILE_RE), reg_start)],
        states={
            STATE_REG_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_name)],
            STATE_REG_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_phone)],
            STATE_REG_WORK: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_work)],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
    )
    app.add_handler(reg_conv)

    consult_conv = ConversationHandler(
        entry_points=[CommandHandler("new", new_start), MessageHandler(filters.Regex(BTN_NEW_CONSULT_RE), new_start)],
        states={
            STATE_COMPLAINTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, new_complaints)],
            STATE_HISTORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, new_history)],
            STATE_PLAN: [MessageHandler(filters.TEXT & ~filters.COMMAND, new_plan)],
            STATE_FILES: [
                MessageHandler(filters.PHOTO | filters.Document.ALL, new_files),
                MessageHandler(filters.Regex("^Готово$"), new_done),
            ],
            STATE_CONFIRM: [
                MessageHandler(filters.Regex("^✅ Отправить$"), new_confirm),
                MessageHandler(filters.Regex("^❌ Отмена$"), new_confirm),
                MessageHandler(filters.Regex("^🔄 Начать заново$"), new_confirm),
                MessageHandler(filters.Regex("^▶️ Продолжить$"), new_confirm),
            ],
        },
        fallbacks=[CommandHandler("cancel", lambda u, c: ConversationHandler.END)],
    )
    app.add_handler(consult_conv)

    app.add_handler(MessageHandler(filters.Regex(BTN_MY_DATA_RE), cmd_me))
    app.add_handler(CallbackQueryHandler(cb_view_consult, pattern=r"^view_consult:\d+$"))

    return app


def main():
    asyncio.run(db.init_db())
    app = build_application()
    log.info("Запуск long polling...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
