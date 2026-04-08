import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, ChatMemberHandler
from database import Database
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

db = Database()

# ─── Helpers ───────────────────────────────────────────────────────────────────

def mention(user) -> str:
    """Создаёт упоминание пользователя."""
    name = user.get("first_name", "")
    last = user.get("last_name", "")
    full = (name + " " + last).strip() or user.get("username") or "User"
    uid = user["user_id"]
    return f"[{full}](tg://user?id={uid})"

async def tag_users(update: Update, users: list, label: str):
    """Отправляет сообщение с упоминаниями, разбивая на части по 30."""
    if not users:
        await update.message.reply_text(f"❌ Нет участников в группе «{label}».")
        return

    chunk_size = 30
    for i in range(0, len(users), chunk_size):
        chunk = users[i:i + chunk_size]
        mentions = "  ".join(mention(u) for u in chunk)
        part = f"*{label}* ({i+1}–{i+len(chunk)}):\n{mentions}"
        await update.message.reply_text(part, parse_mode="Markdown")

# ─── Commands ──────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 *Бот для упоминаний*\n\n"
        "📌 *Команды:*\n"
        "`/all` — отметить всех\n"
        "`/men` — отметить мужчин\n"
        "`/women` — отметить женщин\n"
        "`/group <название>` — отметить произвольную группу\n\n"
        "⚙️ *Управление участниками:*\n"
        "`/addme men` — добавить себя в группу «Мужчины»\n"
        "`/addme women` — добавить себя в группу «Женщины»\n"
        "`/addme <название>` — добавить себя в произвольную группу\n"
        "`/adduser @username men` — добавить другого пользователя (только для админов)\n"
        "`/removeme` — убрать себя из всех групп\n"
        "`/mygroups` — посмотреть, в каких группах вы состоите\n"
        "`/groups` — список всех групп в этом чате\n"
        "`/members <название>` — список участников группы\n"
    )
    await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    users = db.get_all_users(chat_id)
    await tag_users(update, users, "Все участники")

async def cmd_men(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    users = db.get_group_users(chat_id, "men")
    await tag_users(update, users, "Мужчины")

async def cmd_women(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    users = db.get_group_users(chat_id, "women")
    await tag_users(update, users, "Женщины")

async def cmd_group(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("❗ Укажите название группы: `/group <название>`", parse_mode="Markdown")
        return
    group_name = " ".join(ctx.args).lower()
    chat_id = update.effective_chat.id
    users = db.get_group_users(chat_id, group_name)
    await tag_users(update, users, group_name.capitalize())

async def cmd_addme(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("❗ Укажите группу: `/addme men` или `/addme women` или `/addme <название>`", parse_mode="Markdown")
        return
    group_name = " ".join(ctx.args).lower()
    user = update.effective_user
    chat_id = update.effective_chat.id
    db.add_user(chat_id, user.id, user.first_name, user.last_name or "", user.username or "", group_name)
    await update.message.reply_text(f"✅ Вы добавлены в группу *{group_name}*!", parse_mode="Markdown")

async def cmd_adduser(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Только для администраторов: /adduser @username <группа>"""
    chat_id = update.effective_chat.id
    member = await update.effective_chat.get_member(update.effective_user.id)
    if member.status not in ("administrator", "creator"):
        await update.message.reply_text("❌ Только администраторы могут добавлять других пользователей.")
        return

    if len(ctx.args) < 2:
        await update.message.reply_text("❗ Использование: `/adduser @username <группа>`", parse_mode="Markdown")
        return

    username = ctx.args[0].lstrip("@")
    group_name = " ".join(ctx.args[1:]).lower()

    # Ищем пользователя в базе по username
    found = db.find_by_username(chat_id, username)
    if not found:
        await update.message.reply_text(
            f"❌ Пользователь @{username} не найден в базе.\n"
            "Попросите его написать `/addme` в этом чате."
        )
        return

    db.add_user(chat_id, found["user_id"], found["first_name"], found["last_name"], username, group_name)
    await update.message.reply_text(f"✅ @{username} добавлен в группу *{group_name}*!", parse_mode="Markdown")

async def cmd_removeme(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id
    db.remove_user(chat_id, user.id)
    await update.message.reply_text("✅ Вы удалены из всех групп этого чата.")

async def cmd_mygroups(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat_id = update.effective_chat.id
    groups = db.get_user_groups(chat_id, user.id)
    if not groups:
        await update.message.reply_text("ℹ️ Вы не состоите ни в одной группе. Напишите `/addme <группа>`.", parse_mode="Markdown")
    else:
        text = "📋 *Ваши группы:*\n" + "\n".join(f"• {g}" for g in groups)
        await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_groups(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    groups = db.get_all_groups(chat_id)
    if not groups:
        await update.message.reply_text("ℹ️ В этом чате ещё нет групп.")
    else:
        lines = [f"• *{g['name']}* — {g['count']} чел." for g in groups]
        text = "📋 *Группы в этом чате:*\n" + "\n".join(lines)
        await update.message.reply_text(text, parse_mode="Markdown")

async def cmd_members(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("❗ Укажите группу: `/members <название>`", parse_mode="Markdown")
        return
    group_name = " ".join(ctx.args).lower()
    chat_id = update.effective_chat.id
    users = db.get_group_users(chat_id, group_name)
    if not users:
        await update.message.reply_text(f"ℹ️ Группа *{group_name}* пуста или не существует.", parse_mode="Markdown")
        return
    lines = []
    for u in users:
        name = (u["first_name"] + " " + u["last_name"]).strip()
        uname = f" (@{u['username']})" if u.get("username") else ""
        lines.append(f"• {name}{uname}")
    text = f"👥 *{group_name.capitalize()}* ({len(users)}):\n" + "\n".join(lines)
    await update.message.reply_text(text, parse_mode="Markdown")

# ─── Entry ─────────────────────────────────────────────────────────────────────

def main():
    import os
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise ValueError("Установите переменную окружения BOT_TOKEN")

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("all", cmd_all))
    app.add_handler(CommandHandler("men", cmd_men))
    app.add_handler(CommandHandler("women", cmd_women))
    app.add_handler(CommandHandler("group", cmd_group))
    app.add_handler(CommandHandler("addme", cmd_addme))
    app.add_handler(CommandHandler("adduser", cmd_adduser))
    app.add_handler(CommandHandler("removeme", cmd_removeme))
    app.add_handler(CommandHandler("mygroups", cmd_mygroups))
    app.add_handler(CommandHandler("groups", cmd_groups))
    app.add_handler(CommandHandler("members", cmd_members))

    logger.info("Бот запущен...")
    app.run_polling()

if __name__ == "__main__":
    main()
