"""empty message

Revision ID: 7457c9553061
Revises: b85cd1a48e6c
Create Date: 2019-03-26 17:53:41.244095

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '7457c9553061'
down_revision = 'b85cd1a48e6c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('results',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('task_type', sa.String(length=255), nullable=False),
    sa.Column('task_parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('result', sa.JSON(), nullable=True),
    sa.Column('last_updated', sa.DateTime(), nullable=True),
    sa.Column('last_accessed', sa.DateTime(), nullable=True),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('task_type', 'task_parameters', name='uq_results_task_type_task_parameters')
    )
    op.drop_table('queries')
    op.add_column('tasks', sa.Column('task_parameters', postgresql.JSONB(astext_type=sa.Text()), nullable=False))
    op.add_column('tasks', sa.Column('task_type', sa.String(length=255), nullable=False))
    op.drop_column('tasks', 'query_type')
    op.drop_column('tasks', 'query_parameters')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('tasks', sa.Column('query_parameters', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=False))
    op.add_column('tasks', sa.Column('query_type', sa.VARCHAR(length=255), autoincrement=False, nullable=False))
    op.drop_column('tasks', 'task_type')
    op.drop_column('tasks', 'task_parameters')
    op.create_table('queries',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('query_type', sa.VARCHAR(length=255), autoincrement=False, nullable=False),
    sa.Column('query_parameters', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=False),
    sa.Column('query_result', postgresql.JSON(astext_type=sa.Text()), autoincrement=False, nullable=True),
    sa.Column('last_updated', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('last_accessed', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('id', name='queries_pkey'),
    sa.UniqueConstraint('query_type', 'query_parameters', name='uq_queries_query_type_query_parameters')
    )
    op.drop_table('results')
    # ### end Alembic commands ###