from pyflink.common import WatermarkStrategy, Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor

#1. Create streamexecutionenvironment
env = StreamExecutionEnvironment.get_execution_environment()
# enable checkpointing
env.enable_checkpointing(1000) # 1 sec

# State custom logic implementation
class MyMapFunction(MapFunction):
    # open is called once per STATE
    # if we have 2 states, then open is called 2 times 
    # if we have 4 states, open called 4 times [one time per state]
    # runtime_context is useful to get state
    # the state may be stored in memory or rocks db etc based on app configuration
    # while writing state functions, we don't consider full/partial checkpoint or whether DFS used or not
    # all check point, state backend is in configuration
    # ValueStateDescriptor is type of state, it stores one value at any time
    # A MapFunction can have any state parameter, defiend by name
    # cnt - is one state, hold one value per state
    # like cnt, we can create as many state possible
    # state can be anytime
    # called one time per state
    def open(self, runtime_context: RuntimeContext):
        print("MyMapFunction open")
        # create 1 value state descriptor wiht long type
        state_desc = ValueStateDescriptor('cnt', Types.LONG())
        #Define value state
        # try to get initial state
        # typically this value shall be undefined very first time
        # if the state is restored from checkpoint or 
        # if the state is restored from save point, the state value is based on check point value means non null
        # self.cnt_state will differ for each state, every state has one cnt_state
        self.cnt_state = runtime_context.get_state(state_desc)
        print ("Count state is ", self.cnt_state)
        
    # business logic , called for every data event/input/element, grouped by state
    
    def map(self, value):
        # get the latest value from state
        # first time, value can be None
        # cnt is python data variable
        cnt = self.cnt_state.value()
        print("MyMapFunction map", cnt)
        # function called first tiem for the state, we initialize count 0
        if cnt is None:
            cnt = 0
        # increment the count by 1
        # new_cnt is in python local function memory, not in flink state
        new_cnt = cnt + 1
        # update is flink state function to update the state in flink
        # updated value may be stored in memory or rocks db or pushed to DFS based on configuration
        
        self.cnt_state.update(new_cnt)
        # return tupe (0, 1) etc
        # value is a Types.ROW , value[0] is the number either 0 or 1 based n % 2
        return value[0], new_cnt
    
#2. Create data source
seq_num_source = NumberSequenceSource(1, 10)

# ds - data stream , previously we saw table in batch mode
ds = env.from_source(
    source=seq_num_source,
    watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    source_name='seq_num_source',
    type_info=Types.LONG())

# Output is tranformation function
# it parse json data and generate python object/java/ may generate pojo
# data conversation, input and output may vary
# map doesn't filter data, n inputs = n output of same or different type
# must provide output type for map
tempds = ds.map (lambda n: n * 10, output_type=Types.LONG())
tempds.print()
env.execute()

# structured type, tuple data, group of elements
# Defined using Row, not all types  seen in DataTypes from table 
# mapped here, parity diff between table api types and data stream types 
# define a row, that returns actual value n , boolean True or False for odd/even
tempds = ds.map (lambda n: Row(n, n % 2 == 0), 
                     output_type= Types.ROW([Types.LONG(), Types.BOOLEAN()]))

tempds.print()
env.execute()

# tuple 
tempds = ds.map (lambda n: (n, n % 2 == 0), 
                     output_type= Types.TUPLE([Types.LONG(), Types.BOOLEAN()]))

tempds.print()
env.execute()

#3. Define execution logic
# key_by method is useful to create state operation
# key_by to return a value from input element/event
# returned value is useful to maintain separate state 
# if returned value is 0 - it maintain separate state for 0
# if returned value is 1 - it maintain separate state for 1
# if returned value is country code, it maintain state for each country code
# state = DATA + DATA HANDLING LOGIC [add/update/delete] + BUSINESS LOGIC
# state, logic all applied based on key_by return value
# n is input value 1, 2, 3...10
# a is row , the first elemetis mod n % 2, like Row(1, 1) or Row(0, 1) etc
# a[0] is first element in array it can be either 0 or 1
# key_by to maintain two states ,  for odd number 1,  for even number 0
# almost many functions in flink helps to maintain state on keyed stream
# ds.map is data stream
# .key_by, return value is keyed stream based on state formation by key_by
# MyMapFunction is a custom state implementation, it can't be lambda
# next to MyMapFunction, we have return data type of mymapfunction
# the logic of mymapfunction to count unique keys from keyed stream
ds2 = ds.map(lambda n: Row(n % 2, 1), output_type=Types.ROW([Types.LONG(), Types.LONG()])) \
       .key_by(lambda a: a[0]) \
       .map(MyMapFunction(), output_type=Types.TUPLE([Types.LONG(), Types.LONG()]))

ds2.print()
env.execute()
