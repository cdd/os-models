"""
Copyright (C) 2017 Anodyne Informatics, LLC
"""

# see http://code.activestate.com/recipes/252158-how-to-freeze-python-classes/

def frozen(set):
    """Raise an error when trying to set an undeclared name, or when calling
       from a method other than Frozen.__init__ or the __init__ method of
       a class derived from Frozen"""

    def set_attr(self, name, value):
        import sys
        if hasattr(self, name):  # If attribute already exists, simply set it
            set(self, name, value)
            return
        elif sys._getframe(
                1).f_code.co_name is '__init__':  # Allow __setattr__ calls in __init__ calls of proper object types
            for k, v in sys._getframe(1).f_locals.items():
                if k == "self" and isinstance(v, self.__class__):
                    set(self, name, value)
                    return
        raise AttributeError("You cannot add attribute {} to {} ".format(name, type(self).__name__))

    return set_attr


class Frozen(object):
    """Subclasses of Frozen are frozen, i.e. it is impossible to add
     new attributes to them and their instances."""
    __setattr__=frozen(object.__setattr__)
    class __metaclass__(type):
        __setattr__=frozen(type.__setattr__)
