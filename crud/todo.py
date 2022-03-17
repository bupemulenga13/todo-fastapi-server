from typing import List

from sqlalchemy.orm import Session
from models import TodoModel, UserModel
from schemas import TodoSchema, TodoUpdate


def add_todo(
        db: Session,
        current_user: UserModel,
        todo_data: TodoSchema,
):
    todo: TodoModel = TodoModel(
        text=todo_data.text,
        description=todo_data.description,
        completed=todo_data.completed,
    )
    todo.owner = current_user
    db.add(todo)
    db.commit()
    db.refresh(todo)
    return todo


def update_todo(
        db: Session,
        new_todo: TodoUpdate,
):
    todo: TodoModel = db.query(TodoModel).filter(
        TodoModel.id == new_todo.id,
    ).first()
    todo.text = new_todo.text
    todo.completed = new_todo.completed
    db.commit()
    db.refresh(todo)
    return todo


def delete_todo(
        db: Session,
        todo_id: int,
):
    todo: TodoModel = db.query(TodoModel).filter(TodoModel.id == todo_id).first()
    db.delete(todo)
    db.commit()


def get_user_todos(
        db: Session,
        current_user: UserModel,
) -> List[TodoModel]:
    todos = db.query(TodoModel).filter(TodoModel.owner == current_user).all()
    return todos