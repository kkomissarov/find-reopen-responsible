from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, Sequence
from sqlalchemy.sql.expression import exists

engine = create_engine('sqlite:///db.sqlite', echo=False)
Session = sessionmaker(bind=engine)()
Base = declarative_base()


class User(Base):
    __tablename__ = 'user'

    id = Column(Integer, Sequence('id_seq'), primary_key=True)
    roles = Column(String)

    def __repr__(self):
        return f'User#{self.id}'


class UserTree(Base):
    __tablename__ = 'user_tree'
    id = Column(Integer, Sequence('id_seq'), primary_key=True)
    user_id = Column(Integer)
    parent_id = Column(Integer)
    is_strong = Column(Boolean, default=True)

    def __repr__(self):
        return f'User {self.user_id}, Parent {self.parent_id}'

Base.metadata.create_all(engine)

current_responsible_ids = [5, 10]

def find_reopen_responsible(db_session, user_id_list):
    user_list = db_session.query(User)\
                .filter(User.id.in_(user_id_list))\
                .all()

    reopen_responsibles = [user.id for user in user_list if user.roles == 'reopen_responsible']
    return reopen_responsibles[0] if reopen_responsibles else None


def get_strong_parents(db_session, user_id_list):
    trees = db_session.query(UserTree).filter(
        UserTree.user_id.in_(user_id_list),
        UserTree.is_strong==True).all()

    return [user_tree.parent_id for user_tree in trees]


def get_functional_parents(db_session, user_id_list):
    trees = db_session.query(UserTree).filter(
        UserTree.user_id.in_(current_responsible_ids),
        ~UserTree.parent_id.in_(current_responsible_ids),
        UserTree.is_strong==False).all()

    return [user_tree.parent_id for user_tree in trees]


def get_reopen_responsible(db_session, current_responsible_ids):
    new_responsible = None
    check_user_ids = current_responsible_ids

    # Try to find reopen responsible in strong relations
    while check_user_ids:
        new_responsible = find_reopen_responsible(db_session, check_user_ids)
        if new_responsible:
            break
        else:
            check_user_ids = get_strong_parents(db_session, check_user_ids)

    # Try to find reopen responsible in not strong relations
    if not new_responsible:
        functional_parents_ids = get_functional_parents(db_session, current_responsible_ids)
        new_responsible = find_reopen_responsible(db_session,functional_parents_ids)

    return new_responsible if new_responsible else None

print(get_reopen_responsible(Session, current_responsible_ids))
