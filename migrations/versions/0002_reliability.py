"""Отпечатки идемпотентности, блокировки outbox, ретраи webhook и хранение DLQ."""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0002_reliability"
down_revision = "0001_initial"
branch_labels = None
depends_on = None

def upgrade():
    op.add_column("payments", sa.Column("request_fingerprint", sa.String(64), nullable=True))
    # Существующие строки появились до fingerprint. Помечаем как legacy —
    # повтор ключа с другим телом запроса вернёт 409.
    op.execute("UPDATE payments SET request_fingerprint = 'legacy-existing-payment' WHERE request_fingerprint IS NULL")
    op.alter_column("payments", "request_fingerprint", nullable=False)
    op.create_check_constraint("ck_payments_status", "payments", "status IN ('pending', 'succeeded', 'failed')")
    op.create_check_constraint("ck_payments_amount_positive", "payments", "amount > 0")
    op.add_column("outbox", sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True))

    op.create_table(
        "webhook_deliveries",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('pending', 'delivered', 'failed')", name="ck_webhook_deliveries_status"),
    )
    op.create_index("ix_webhook_due", "webhook_deliveries", ["status", "next_attempt_at"])
    op.create_table(
        "dead_letters",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )

def downgrade():
    op.drop_table("dead_letters")
    op.drop_index("ix_webhook_due", table_name="webhook_deliveries")
    op.drop_table("webhook_deliveries")
    op.drop_column("outbox", "locked_at")
    op.drop_constraint("ck_payments_amount_positive", "payments", type_="check")
    op.drop_constraint("ck_payments_status", "payments", type_="check")
    op.drop_column("payments", "request_fingerprint")
