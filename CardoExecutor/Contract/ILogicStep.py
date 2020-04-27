from CardoExecutor.Common.CardoDataFrame import CardoDataFrame
from CardoExecutor.Contract.CardoContextBase import CardoContextBase
from CardoExecutor.Contract.IStep import IStep

from CardoExecutor.Common.GeneralFunctions import snake_case


class ILogicStep(IStep):
    """
    receive a reader and apply a snake_case of a child class names to its CardoDataFrame.table_name

    class MyLogic(ILogicStep):
        def __init__(self):
            super().__init__(reader=OracleReader(...))

    in any workflow factory will behave like:
        example_workflow.create_workflow(MyLogic())
        will return:
        [my_logic]

    """
    node_attributes = {'fillcolor': 'DodgerBlue', 'style': 'filled'}

    def __init__(self, reader: IStep = None, **node_attributes):
        """By default the node_attributes will be the class attribute (how the node will be represented
        in graphviz).
        A user can override it via the __init__ of ILogicStep or the class attribute of any inherited class.
        """
        self.reader = reader
        self.node_attributes = dict(ILogicStep.node_attributes, **node_attributes)

    def process(self, cardo_context: CardoContextBase, cardo_dataframe: CardoDataFrame=None) -> CardoDataFrame:
        if self.reader:
            logic_dataframe = self.reader.process(cardo_context, cardo_dataframe)
            return CardoDataFrame(logic_dataframe, snake_case(self.__class__.__name__))
        else:
            raise NotImplementedError
