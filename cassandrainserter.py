import sys
import subprocess
import pandas as pd


def read_data(path):
    data = None
    if path.endswith('.csv'):
        data = pd.read_csv(path, delimiter=',')
    if path.endswith('.json'):
        data = pd.read_json(path)
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
    data = dfs.array
    out = '('
    delimiter = ', '
    for i in range(len(data)):
        dt = data[i]
        wrd = '\'' + str(data[i]) + '\''
        # if type(dt) != str: # uncomment for python3.x
        if type(dt) != unicode:
            wrd = str(data[i])

        if i == len(data) - 1:
            delimiter = ')'

        out = out + wrd + delimiter
    return out


def parse_columns(cols):
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


class CassandraDataInserter:
    def __init__(self, keyspace, table, data_path=None):
        self.data = None
        self.cols_list = None
        self.cols_str = None
        self.keyspace = keyspace
        self.table_name = table
        self.db = self.get_db()
        self.init_data(data_path)
        self.api = 'cqlsh'

    def init_data(self, path=None):
        if path is not None:
            self.data = read_data(path)
            self.cols_list = self.data.columns.to_list()
            self.cols_str = parse_columns(self.cols_list)

    def get_db(self):
        return self.keyspace + '.' + self.table_name

    def create_single_query(self, data, query_type=None):
        return c('INSERT', 'INTO', self.get_db(), self.cols_str, 'VALUES', parse_data(data))

    def execute(self, command='INSERT'):
        for i in range(len(self.data)):
            process = None
            df = self.data
            try:
                query = self.create_single_query(df.iloc[i])
                c_query = c_arr(self.api, '-e', query)
                print_out('Executing ' + query)
                print(c_query)
                process = subprocess.Popen(args=c_query,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE,
                                           universal_newlines=True)
                stdout, stderr = process.communicate()
                if len(stderr) == 0:
                    print(stdout)
                    print_out('SUCCESS')
                else:
                    print(stderr)
                process.terminate()
            except Exception as ex:
                if ex.filename == self.api:
                    print_out('Error: cqlsh not installed, please check your cassandraDB installation', BColors.WARNING)
                if process is not None:
                    process.terminate()
                break

    def show_commands(self):
        for i in range(len(self.data)):
            df = self.data.iloc[i]
            print(c_arr(self.api, '-e', self.create_single_query(df)))

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
    print(color + word + BColors.ENDC)


def welcome_text():
    print_out('Usage: python cassandrainserter -k [KEYSPACE] -t [TABLE NAME] -f [PATH TO CSV/JSON FILE]')
    print_out('insert data into cassandra db on unix systems from a JSON or CSV file')
    print_out('')
    print_out('To use this script, you should have created your keyspace and Table name')
    print_out('   -k     the keyspace of the collection')
    print_out('   -t     the keyspace table name to insert the data')
    print_out('')
    print_out('Example:')
    print_out('python cassandrainserter -k rainforest -k recordings -f ~/desktop/data.json')
    print_out('')
    print_out('Made by Rex Ijiekhuamen')
    print_out('Because the University\'s VMS do not have internet access \n' +
              'but we are somehow meant to insert tons of data to a database')


if __name__ == '__main__':
    arguments = sys.argv
    args_dict = None
    try:
        args_dict = {
            'KEYSPACE': arguments[arguments.index('-k') + 1],
            'TABLE': arguments[arguments.index('-t') + 1],
            'FILE': arguments[arguments.index('-f') + 1]
        }
    except Exception as e:
        welcome_text()
        exit()

    ins = CassandraDataInserter(keyspace=args_dict['KEYSPACE'],
                                table=args_dict['TABLE'],
                                data_path=args_dict['FILE'])

    ins.execute()
