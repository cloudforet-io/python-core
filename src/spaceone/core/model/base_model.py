from typing import Tuple, List


class BaseModel(object):

    @classmethod
    def init(cls):
        """
        Args:
        Returns:
            None
        """
        raise NotImplementedError('model.init not implemented!')

    @classmethod
    def create(cls, data: dict):
        """
        Args:
            data (dict)
        Returns:
            model_vo (object)
        """
        raise NotImplementedError('model.create not implemented!')

    def update(self, data: dict):
        """
        Args:
            data (dict)
        Returns:
            model_vo (object)
        """
        raise NotImplementedError('model.update not implemented!')

    def delete(self):
        """
        Args:
        Returns:
            None
        """
        raise NotImplementedError('model.delete not implemented!')

    def terminate(self):
        """
        Args:
        Returns:
            None
        """
        raise NotImplementedError('model.terminate not implemented!')

    def increment(self, key: str, amount: int = 1) -> object:
        """
        Args:
            key (str)
            amount (int)
        Returns:
            model_vo (object)
        """
        raise NotImplementedError('model.increment not implemented!')

    def decrement(self, key: str, amount: int = 1):
        """
        Args:
            key (str)
            amount (int)
        Returns:
            model_vo (object)
        """
        raise NotImplementedError('model.decrement not implemented!')

    @classmethod
    def get(cls, **conditions):
        """
        Args:
            **conditions (kwargs)
            - key (str): value (any)
        Returns:
            model_vo (object)
        """
        raise NotImplementedError('model.get not implemented!')

    @classmethod
    def filter(cls, **conditions):
        """
        Args:
            **conditions (kwargs)
            - key (str): value (any)
        Returns:
            model_vos (list)
        """
        raise NotImplementedError('model.filter not implemented!')

    def to_dict(self):
        """
        Args:
        Returns:
            model_data (dict)
        """
        raise NotImplementedError('model.to_dict not implemented!')

    @classmethod
    def query(
            cls,
            *args,
            only: list = None,
            exclude: list = None,
            filter: list = None,
            filter_or: list = None,
            sort: dict = None,
            page: dict = None,
            minimal: bool = False,
            count_only: bool = False,
            **kwargs
    ):
        """
        Args:
            *args (list)
            only (list)
            exclude (list)
            filter (list)
            filter_or (list)
            sort (dict)
            page (dict)
            minimal (bool)
            count_only (bool)
            **kwargs (kwargs)

        Returns:
            model_vos (list)
            total_count (int)
        """
        raise NotImplementedError('model.query not implemented!')

    @classmethod
    def analyze(
            cls,
            *args,
            granularity: str = None,
            fields: dict = None,
            select: dict = None,
            group_by: list = None,
            field_group: list = None,
            filter: list = None,
            filter_or: list = None,
            page: dict = None,
            sort: dict = None,
            start: str = None,
            end: str = None,
            date_field: str = 'date',
            date_field_format: str = '%Y-%m-%d',
            **kwargs
    ):
        """
        Args:
            *args (list)
            granularity (str)
            fields (dict)
            select (dict)
            group_by (list)
            field_group (list)
            filter (list)
            filter_or (list)
            page (dict)
            sort (dict)
            start (str)
            end (str)
            date_field (str)
            date_field_format (str)
            **kwargs (kwargs)

        Returns:
            results (list)
            more (bool)
        """
        raise NotImplementedError('model.analyze not implemented!')
