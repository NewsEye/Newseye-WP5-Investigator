"""empty message

Revision ID: fd6994125112
Revises: 33bbb70ea77e
Create Date: 2019-07-23 14:24:00.007657

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fd6994125112'
down_revision = '33bbb70ea77e'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('task_instances', sa.Column('task_status', sa.String(length=255), nullable=True))
    op.drop_column('tasks', 'task_status')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('tasks', sa.Column('task_status', sa.VARCHAR(length=255), autoincrement=False, nullable=True))
    op.drop_column('task_instances', 'task_status')
    # ### end Alembic commands ###