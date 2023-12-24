from spaceone.core.model.mongo_model import MongoModel


def init_all(create_index: bool = True) -> None:
    MongoModel.init(create_index)
