"""Improve the timestamps and remove some obsolete fields

Revision ID: 35654ff50445
Revises: 7457c9553061
Create Date: 2019-04-08 15:04:44.542383

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '35654ff50445'
down_revision = '7457c9553061'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('results', 'last_accessed')
    op.add_column('tasks', sa.Column('task_finished', sa.DateTime(), nullable=True))
    op.add_column('tasks', sa.Column('task_started', sa.DateTime(), nullable=True))
    op.drop_column('tasks', 'last_updated')
    op.drop_constraint('users_current_task_id_fkey', 'users', type_='foreignkey')
    op.drop_column('users', 'current_task_id')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('users', sa.Column('current_task_id', sa.INTEGER(), autoincrement=False, nullable=True))
    op.create_foreign_key('users_current_task_id_fkey', 'users', 'tasks', ['current_task_id'], ['id'])
    op.add_column('tasks', sa.Column('last_updated', postgresql.TIMESTAMP(), autoincrement=False, nullable=True))
    op.drop_column('tasks', 'task_started')
    op.drop_column('tasks', 'task_finished')
    op.add_column('results', sa.Column('last_accessed', postgresql.TIMESTAMP(), autoincrement=False, nullable=True))
    # ### end Alembic commands ###