"""split Task.task_parameters into separate fields: utility_name, search and utility_parameters

Revision ID: 910a42b39d3f
Revises: 0c6e8efa05f7
Create Date: 2019-07-24 11:59:18.431348

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '910a42b39d3f'
down_revision = '0c6e8efa05f7'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('tasks', sa.Column('search', postgresql.JSONB(astext_type=sa.Text()), nullable=False))
    op.add_column('tasks', sa.Column('utility_name', sa.String(length=255), nullable=True))
    op.add_column('tasks', sa.Column('utility_parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.drop_constraint('uq_task_type_task_parameters', 'tasks', type_='unique')
    op.create_unique_constraint('uq_task_type_task_parameters', 'tasks', ['task_type', 'utility_name', 'search', 'utility_parameters'])
    op.drop_column('tasks', 'task_parameters')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('tasks', sa.Column('task_parameters', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=False))
    op.drop_constraint('uq_task_type_task_parameters', 'tasks', type_='unique')
    op.create_unique_constraint('uq_task_type_task_parameters', 'tasks', ['task_type', 'task_parameters'])
    op.drop_column('tasks', 'utility_parameters')
    op.drop_column('tasks', 'utility_name')
    op.drop_column('tasks', 'search')
    # ### end Alembic commands ###
