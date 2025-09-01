from .command_handler import router as command_router
from .file_handler import router as file_router
from .smartject_manager import router as smartject_router

__all__ = ['command_router', 'file_router', 'smartject_router']
