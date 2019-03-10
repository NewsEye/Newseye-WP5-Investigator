"""Add data_source_id to Task fields

Revision ID: 1fb43417d846
Revises: c9adeb1cd228
Create Date: 2019-03-08 12:59:42.778359

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '1fb43417d846'
down_revision = 'c9adeb1cd228'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('tasks', sa.Column('data_source_id', sa.Integer(), nullable=True))
    op.create_foreign_key(None, 'tasks', 'tasks', ['data_source_id'], ['id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'tasks', type_='foreignkey')
    op.drop_column('tasks', 'data_source_id')
    # ### end Alembic commands ###
