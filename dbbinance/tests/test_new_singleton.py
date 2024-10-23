class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        unique_name = kwargs.get('unique_name')
        if unique_name is None:
            raise ValueError("unique_name is required")
        if unique_name not in cls._instances:
            cls._instances[unique_name] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[unique_name]


class MyClass(metaclass=Singleton):
    def __init__(self, unique_name='one'):
        self.unique_name = unique_name

    def run(self, msg):
        print(msg)


# Create instances with different singleton names
obj1 = MyClass(unique_name='one')
obj2 = MyClass(unique_name='two')
obj3 = MyClass(unique_name='three')
obj4 = MyClass(unique_name='three')

# Print the addresses of the objects
print(id(obj1))  # prints the address of obj1
print(id(obj2))  # prints the address of obj2
print(id(obj3))  # prints the address of obj3
print(id(obj4))  # prints the address of obj3

# Check if obj1, obj2, and obj3 are different objects
print(obj1 is obj2)  # prints: False
print(obj1 is obj3)  # prints: False
print(obj2 is obj3)  # prints: False
print(obj3 is obj4)  # prints: True

obj1.run('Hello, World!')
# Delete an instance
del obj1

# Check if the instance was deleted
print(Singleton._instances)
