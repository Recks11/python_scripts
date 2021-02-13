import sys
import subprocess
import pandas as pd


class RunArgs:
    KEYSPACE = 'KEYSPACE'
    TABLE = 'TABLE'
    FILE = 'FILE'
    INSERT = 'INSERT'
    SHOW = 'SHOW'
    CLEAR_TABLE = 'CLEAR'


def read_data(path):
    data = None
    if path.endswith('.csv'):
        data = pd.read_csv(path, delimiter=',')
    if path.endswith('.json'):
        data = pd.read_json(path)
    print_out('Read data of shape ' + str(data.shape))
    print_out('column names are: ' + str(data.columns.array))
    print_out('Number of rows: ' + str(data.shape[0]))
    return data


def c(*args):
    text = ""
    space = ' '
    for i in range(len(args)):
        if i == len(args) - 1:
            space = ''
        text = text + args[i] + space
    return text


def c_arr(*args):
    array = []
    for i in range(len(args)):
        array.append(args[i])
    return array


def parse_data(dfs):
    """
        Takes a Dataframe and generates values for an insert query
        output like
        ('value1', 'value2', 3, false, 'value5')
    """
    data = dfs.array
    out = '('
    delimiter = ', '
    for i in range(len(data)):
        dt = data[i]
        wrd = '\'' + str(data[i]) + '\''
        # if type(dt) != str:  # uncomment for python3.x
        if type(dt) != unicode:
            wrd = str(data[i])

        if i == len(data) - 1:
            delimiter = ')'

        out = out + wrd + delimiter
    return out


def parse_columns(cols):
    """
        Takes a List of columns and converts them to the format
        (name, name2, name3)
    """
    t = '('
    delimiter = ', '
    for i in range(len(cols)):
        if i == len(cols) - 1:
            delimiter = ')'
        t = t + cols[i] + delimiter
    return t


def parse_type(v):
    if v.dtype == bool:
        return 'BOOLEAN'
    if v.dtype == object:
        return 'VARCHAR'
    if v.dtype == int:
        return 'INT'
    if v.dtype == float:
        return 'DOUBLE'


class QueryType:
    INSERT = 'INSERT'
    SELECT = 'SELECT'
    CLEAR = 'CLEAR'


class QueryGenerator:
    def __init__(self, db, table, data):
        self.database = db
        self.table_name = table
        self.data = data
        self.cols_list = self.data.columns.to_list()
        self.columns = parse_columns(self.cols_list)

    def generate_query(self, query_type, data=None):
        if query_type == QueryType.INSERT:
            return self.gen_insert_query(data)
        if query_type == QueryType.CLEAR:
            return self.gen_clear_query()
        if query_type == QueryType.SELECT:
            return self.gen_select_query()

    def gen_insert_query(self, data):
        return c('INSERT', 'INTO', self.database, self.columns, 'VALUES', parse_data(data))

    def gen_clear_query(self):
        return c('TRUNCATE', self.database)

    def gen_select_query(self):
        return c('SELECT', '*', 'FROM', self.database)


class CassandraDataInserter:
    def __init__(self, keyspace, table, data_path=None):
        self.data = None
        self.keyspace = keyspace
        self.table_name = table
        self.query_gen = None
        self.init_data(data_path)
        self.api = 'cqlsh'

    def init_data(self, path=None):
        if path is not None:
            self.data = read_data(path)
            self.query_gen = QueryGenerator(table=self.table_name,
                                            db=self.get_db(),
                                            data=self.data)

    def get_db(self):
        return self.keyspace + '.' + self.table_name

    def create_single_query(self, data, query_type=None):
        return self.query_gen.generate_query(query_type, data)

    def create_query_command(self, query):
        return c_arr(self.api, '-e', query)
        # return c_arr('docker', 'exec', '-i', 'cassandradb', self.api, '-e', query)

    def execute_command(self, q_type=None, data=None):
        query = self.query_gen.generate_query(q_type, data=data)
        c_query = self.create_query_command(query)
        print_out('Executing ' + query, BColors.WARNING)
        process = subprocess.Popen(args=c_query,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   universal_newlines=True)
        stdout, stderr = process.communicate()
        if len(stderr) == 0:
            if len(stdout) != 0:
                print(stdout)
            return True
        else:
            print_out('Error: ' + stderr, BColors.FAIL)
            return False

    def execute(self, command=QueryType.INSERT):
        df = self.data
        tot = len(self.data)
        for i in range(tot):
            try:
                print_out('Inserting ' + str(i + 1) + '/' + str(tot), BColors.OKBLUE)
                response = self.execute_command(q_type=command, data=df.iloc[i])
                if response:
                    print_out('SUCCESSFULLY INSERTED: ')
                    print_out(str(df.iloc[i]), BColors.OKGREEN)
                else:
                    break

            except Exception as ex:
                if ex.args[1] == 'No such file or directory':
                    print_out('Error: cqlsh not installed, please check your cassandraDB installation', BColors.FAIL)
                if ex.message is not None:
                    print_out('Error executing query: ' + ex.message, BColors.FAIL)
                break
        self.show_all()

    def show_all(self):
        _ = self.execute_command(q_type=QueryType.SELECT)

    def clear_db(self):
        _ = self.execute_command(q_type=QueryType.CLEAR)

    def show_commands(self):
        for i in range(len(self.data)):
            df = self.data.iloc[i]
            print(c_arr(self.api, '-e', self.create_query_command(self.create_single_query(df))))

    def show_queries(self):
        for i in range(len(self.data)):
            df = self.data.iloc[i]
            print(self.create_single_query(df))


class BColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_out(word, color=BColors.OKGREEN):
    if verbose or (color is BColors.OKBLUE or color is BColors.FAIL):
        print(color + word + BColors.ENDC)


def welcome_text():
    print('Usage: python cassandrainserter -k [KEYSPACE] -t [TABLE NAME] -f [PATH TO CSV/JSON FILE]')
    print('insert data into cassandra db on unix systems from a JSON or CSV file')
    print('')
    print('To use this script, you should have created your keyspace and Table name')
    print('   -i | -I to run INTERACTIVE SHELL')
    print('   -k      the keyspace of the collection')
    print('   -t      the keyspace table name to insert the data')
    print('   -v | -V   verbose (show queries and all info  not just errors')
    print('')
    print('Example:')
    print('python cassandrainserter -v -k rainforest -k recordings -f ~/desktop/data.json')
    print('')
    print('or')
    print('python cassandrainserter -i')
    print('')
    print('Made by Rex Ijiekhuamen')
    print('Because the University\'s VMS do not have internet access \n' +
          'but we are somehow meant to insert tons of data to cassandradb')
    print('')


def user_input_welcome():
    print_out('Python cassandra inserter:')
    print_out('')


def get_user_input(arg_dict):
    user_input_welcome()
    arg_dict[RunArgs.KEYSPACE] = raw_input('Please enter the keyspace: ')
    arg_dict[RunArgs.TABLE] = raw_input('Please enter the keyspace: ')
    arg_dict[RunArgs.FILE] = raw_input('Please enter the path to file: ')
    res = raw_input('do you want verbose output? (y/n): ')
    clr = raw_input('do you want to clear the table first? (y/n): ')
    arg_dict[RunArgs.SHOW] = True if res.lower() == 'y' else False
    arg_dict[RunArgs.CLEAR_TABLE] = True if clr.lower() == 'y' else False
    return arg_dict


def read_manual_input(args):
    try:
        _args_dict = {
            RunArgs.KEYSPACE: args[args.index('-k') + 1],
            RunArgs.TABLE: args[args.index('-t') + 1],
            RunArgs.FILE: args[args.index('-f') + 1],
            RunArgs.CLEAR_TABLE: False,
            RunArgs.SHOW: False
        }
        if args.__contains__('--clear' or '-c'):
            _args_dict[RunArgs.CLEAR_TABLE] = True

        if args.__contains__('-v' or '-V'):
            _args_dict[RunArgs.SHOW] = True
        return _args_dict

    except Exception as e:
        welcome_text()
        exit()


verbose = False

if __name__ == '__main__':
    arguments = sys.argv
    args_dict = {
        RunArgs.KEYSPACE: 'None',
        RunArgs.TABLE: None,
        RunArgs.FILE: None,
    }

    if arguments.__contains__('-i') | arguments.__contains__('-I'):
        args_dict = get_user_input(args_dict)
    else:
        args_dict = read_manual_input(arguments)

    verbose = args_dict[RunArgs.SHOW]
    args_dict[RunArgs.INSERT] = True

    ins = CassandraDataInserter(keyspace=args_dict[RunArgs.KEYSPACE],
                                table=args_dict[RunArgs.TABLE],
                                data_path=args_dict[RunArgs.FILE])

    if args_dict[RunArgs.CLEAR_TABLE]:
        ins.clear_db()
    if args_dict[RunArgs.INSERT]:
        ins.execute()

