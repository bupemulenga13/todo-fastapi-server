from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from db.base_class import Base

if TYPE_CHECKING:
    from .user import User  # noqa: F401


class UserModel(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    first_name = Column(String)
    first_name = Column(String)
    email = Column(String, unique=True, index=True)
    # One-to-many relationship with the "todos" table
    todos = relationship("TODO", back_populates="owner", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<User(username='{self.first_name}', email='{self.email}')>"