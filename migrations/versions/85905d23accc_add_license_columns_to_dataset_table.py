"""add license columns to dataset table

Revision ID: 85905d23accc
Revises: aaabf849b37f
Create Date: 2025-02-05 09:45:04.056529

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "85905d23accc"
down_revision: Union[str, None] = "aaabf849b37f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column("dataset", sa.Column("license_url", sa.String(), nullable=True))
    op.add_column("dataset", sa.Column("license_name", sa.String(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("dataset", "license_name")
    op.drop_column("dataset", "license_url")
    # ### end Alembic commands ###
