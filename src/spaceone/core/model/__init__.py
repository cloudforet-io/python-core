class BaseModel(object):

    @classmethod
    def connect(cls):
        """ 
        Args:
        Returns:
            None
        """
        raise NotImplementedError('model.connect not implemented!')

    @classmethod
    def create(cls, data):
        """ 
        Args:
            data (dict)
        Returns:
            model_vo (object)
        """
        raise NotImplementedError('model.create not implemented!')

    def update(self, data):
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

    def increment(self, key, amount=1):
        """
        Args:
            key (str)
            amount (int)
        Returns:
            model_vo (object)
        """
        raise NotImplementedError('model.increment not implemented!')

    def decrement(self, key, amount=1):
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
    def query(cls, **query):
        """
        Args:
            **query (kwargs)
                - filter (list)
                [
                    {
                        'key' : 'field (str)',
                        'value' : 'value (any)',
                        'operator' : 'lt | lte | gt | gte | eq | not | exists | contain |
                        not_contain | in | not_in | not_contain_in | match | regex | regex_in |
                        datetime_lt | datetime_lte | datetime_gt | datetime_gte |
                        timediff_lt | timediff_lte | timediff_gt | timediff_gte'
                    },
                    ...
                ]
                - filter_or (list)
                [
                    {
                        'key' : 'field (str)',
                        'value' : 'value (any)',
                        'operator' : 'lt | lte | gt | gte | eq | not | exists | contain |
                        not_contain | in | not_in | not_contain_in | match | regex | regex_in |
                        datetime_lt | datetime_lte | datetime_gt | datetime_gte |
                        timediff_lt | timediff_lte | timediff_gt | timediff_gte'
                    },
                    ...
                ]
                - sort (dict)
                {
                  'key' : 'field (str)',
                  'desc' : True | False
                }
                - page (dict)
                {
                    'start': 'start_row (int)',
                    'limit' : 'row_limit (int)'
                }
                - distinct (str): 'field'
                - only (list): ['field1', 'field2', '...']
                - exclude(list): ['field1', 'field2', '...']
                - minimal (bool)
                - count_only (bool)

        Returns:
            model_vos (list)
            total_count (int)
        """
        raise NotImplementedError('model.query not implemented!')

    @classmethod
    def stat(cls, **query):
        """
        Args:
            **query (kwargs)
                - filter (list)
                [
                    {
                        'key' : 'field (str)',
                        'value' : 'value (any)',
                        'operator' : 'lt | lte | gt | gte | eq | not | exists | contain | not_contain |
                        in | not_in | contain_in | not_contain_in | match | regex | regex_in |
                        datetime_lt | datetime_lte | datetime_gt | datetime_gte |
                        timediff_lt | timediff_lte | timediff_gt | timediff_gte'
                    },
                    ...
                ]
                - filter_or(list)
                [
                    {
                        'key' : 'field (str)',
                        'value' : 'value (any)',
                        'operator' : 'lt | lte | gt | gte | eq | not | exists | contain | not_contain |
                        in | not_in | contain_in | not_contain_in | match | regex | regex_in |
                        datetime_lt | datetime_lte | datetime_gt | datetime_gte |
                        timediff_lt | timediff_lte | timediff_gt | timediff_gte'
                    },
                    ...
                ]
                - aggregate (dict)
                {
                    'unwind': [
                        {
                            'path': 'key path (str)'
                        }
                    ],
                    'group': {
                        'keys': [
                            {
                                'key': 'field (str)',
                                'name': 'alias name (str)'
                            },
                            ...
                        ],
                        'fields': [
                            {
                                'key': 'field (str)',
                                'name': 'alias name (str)',
                                'operator': 'count | sum | avg | max | min | size | add_to_set | merge_objects'
                            },
                            ...
                        ]
                    }
                    'count': {
                        'name': 'alias name (str)'
                    }
                }
                - sort(dict)
                {
                  'name' : 'field (str)',
                  'desc' : True | False
                }
                - page(dict)
                {
                    'start': 'start_row (int)',
                    'limit' : 'row_limit (int)'
                }

        Returns:
            values (list)
        """
        raise NotImplementedError('model.stat not implemented!')
