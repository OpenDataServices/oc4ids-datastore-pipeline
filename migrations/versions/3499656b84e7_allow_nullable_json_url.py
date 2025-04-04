"""allow nullable json_url

Revision ID: 3499656b84e7
Revises: 084c39bf418e
Create Date: 2025-02-11 16:44:30.550413

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "3499656b84e7"
down_revision: Union[str, None] = "084c39bf418e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column("dataset", "json_url", existing_type=sa.VARCHAR(), nullable=True)
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column("dataset", "json_url", existing_type=sa.VARCHAR(), nullable=False)
    # ### end Alembic commands ###
