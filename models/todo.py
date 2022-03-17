
from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import relationship

from db.base_class import Base

class TodoModel(Base):
   __tablename__ = "todos"
   id = Column(Integer, primary_key=True, index=True)
   text = Column(String, index=True)
   description = Column(String)
   completed = Column(Boolean, default=False)
   owner_id = Column(Integer, ForeignKey("users.id"))
   owner = relationship("User", back_populates="todos")

   def __repr__(self):
         return f"<TODO(text='{self.text}', completed={self.completed})>"