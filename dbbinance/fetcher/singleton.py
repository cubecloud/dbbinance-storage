class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        unique_name = kwargs.get('unique_name')
        if unique_name is None:
            raise ValueError("unique_name is required")
        if unique_name not in cls._instances:
            cls._instances[unique_name] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[unique_name]
    
