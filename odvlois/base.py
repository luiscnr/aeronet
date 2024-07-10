import numpy as np
import pandas as pd
import io
import xmltodict
import logging
from bs4 import BeautifulSoup
from datetime import datetime as dt

log = logging.getLogger('pyodv')


class ODV_Struct(object):
    def __init__(self, odv_path):

        self.init_vars()

        # Read text file
        self.file_path = odv_path
        self.read_odv_file(odv_path)

        # Get ODV protocol details
        self.odv_format()

        # Check Input Validity
        if not self.valid_input():
            log.warning('Bad Input file')

        # Make Valid if not valid
        # TODO: Figure out the edge cases that break the normal flow and fix'em

        # Get some ODV columns
        self.split_columns()

        # Parse file into meaningful struct
        self.parse_odv_file()

        # Parse the header into something useful:
        self.parse_header()

        # Check Output Validity
        self.valid_output()

    def init_vars(self):
        '''
        Just here to keep track of all the variables contained in the class...
        Not very pythonic.
        '''
        self.cols_data = []
        self.file_path = ''
        self.odv_header = ''
        self.odv_df = pd.DataFrame()
        self.df_data = pd.DataFrame()
        self.df_var = pd.DataFrame()
        self.df_qc = pd.DataFrame()
        self.cols_data = []
        self.cols_quality = []
        self.cols_variable = []
        self.comments = []
        self.cols = {}
        self.valid_bio_odv = False
        self.valid_odv = False

    def read_odv_file(self, odv_path):
        '''
        Read file into pandas dataframe without
        doing too much guesswork on structure
        '''
        try:
            # Try to read it with UTF-8 encoding, if that fails
            # try to read it with Latin-1 encoding. If that fails
            # just read it with UTF-8 and ignore any errors.
            with open(odv_path, encoding='utf8') as f:
                lines = f.read()

            split = lines.rsplit('\n//', 1)
            table = split[1].strip()
            self.odv_df = pd.read_csv(io.StringIO(table), sep='\t')

        except UnicodeDecodeError:
            try:
                with open(odv_path, encoding='latin1') as f:
                    lines = f.read()
                split = lines.rsplit('\n//', 1)
                table = split[1].strip()
                self.odv_df = pd.read_csv(io.StringIO(table), sep='\t', encoding='latin1')

            except:
                with open(odv_path, encoding='utf8', errors="ignore") as f:
                    lines = f.read()
                split = lines.rsplit('\n//', 1)
                table = split[1].strip()
                self.odv_df = pd.read_csv(table, sep='\t')

        split = lines.rsplit('\n//', 1)
        table = split[1].strip()
        self.odv_df = pd.read_csv(io.StringIO(table), sep='\t')
        self.odv_header = split[0]
        return

    def odv_format(self):
        '''
        Store the protocol specific information for ODV and bio-ODV
        '''
        self.mandatory_columns = ['Cruise',
                                  'Station',
                                  'Type',
                                  'Longitude [degrees_east]',
                                  'Latitude [degrees_north]',
                                  'LOCAL_CDI_ID',
                                  'EDMO_code',
                                  'Bot. Depth [m]', ]

        timestamp_col = self.get_timestamp_format()
        if timestamp_col is not None:
            self.mandatory_columns.append(timestamp_col)

        self.mandatory_bio_columns = ['MinimumDepthOfObservation [m]',
                                      'MaximumDepthOfObservation [m]',
                                      'SampleID',
                                      'ScientificName',
                                      'ScientificNameID',
                                      'Sex',
                                      'LifeStage',
                                      'ObservedIndividualCount']
        sample_effort_col = self.get_samplingeffort_format()
        if sample_effort_col is not None:
            self.mandatory_bio_columns.append(sample_effort_col)
        return

    def parse_odv_file(self):
        '''
        Split ODV file into table part and metadata part
        return:
          - dataframe with data
          - dataframe with quality variables
          - metadata variables

        Also combine the variables into a multidimensional XArray
        '''
        self.df_data = self.odv_df[self.cols_data].fillna(method='ffill')
        self.df_var = self.odv_df[self.cols_variable]
        self.df_qc = self.odv_df[self.cols_quality]
        return

    def split_columns(self):
        '''
        Get column names for ODV DF for the three catagories of columns:
        return:
          - data columns: generally voyage or station data
          - variable columns: sensor readings for voyage data
          - quality columns: the QC value associated with each reading.
        '''
        columns = self.odv_df.columns
        self.cols_quality = [col for col in columns if col.startswith('QV:')]
        remaining_cols = [x for x in columns if x not in self.cols_quality]
        self.cols_variable = remaining_cols[-len(self.cols_quality):]
        self.cols_data = list(set(self.odv_df.columns) - set(self.cols_variable) - set(self.cols_quality))

        self.cols = {'data': self.cols_data,
                     'variable': self.cols_variable,
                     'quality': self.cols_quality, }
        return

    def get_timestamp_format(self):
        '''
        Get the isoformat used in the column names. This is used to identify the mandatory timestamp column
        as well as to parse the datetime columns.
        '''
        columns = self.odv_df.columns.tolist()
        date_cols = [i for i in columns if i.startswith('YYYY')]
        if len(date_cols) == 0:
            date_cols = [i for i in columns if i.startswith('yyyy')]
        if len(date_cols) == 1:
            date_col = date_cols[0]
        elif len(date_cols) == 0:
            # No columns detected!
            date_col = None
        else:
            # Warning, multiple columns detected!
            date_col = date_cols[0]

        return date_col

    def get_samplingeffort_format(self):
        '''
        Get the SamplingEffort col name used in the file.
        '''
        columns = self.odv_df.columns
        effort_cols = [i for i in columns if i.startswith('SamplingEffort [')]
        if len(effort_cols) == 1:
            effort_col = effort_cols[0]
        elif len(effort_cols) == 0:
            # No columns detected!
            effort_col = None
        return effort_col

    def valid_input(self):
        '''
        Check if file is valid odv.
        TODO: Check if metadata is valid
        '''
        if set(self.mandatory_columns).issubset(set(self.odv_df.columns)):
            self.valid_odv = True
        else:
            print('manca qualche columna', self.odv_df.columns)
            self.valid_odv = False
        self.tmp_data = []
        for mand_col in self.mandatory_bio_columns:
            if mand_col in self.odv_df.columns:
                self.valid_bio_odv = True
                self.tmp_data.append(mand_col)
            elif mand_col + ':INDEXED_TEXT' in self.odv_df.columns:
                self.valid_bio_odv = True
                self.tmp_data.append(mand_col + ':INDEXED_TEXT')
            else:
                self.valid_bio_odv = False

        return self.valid_odv

    def valid_output(self):
        '''
        Run some checks to see that the file was parsed correctly:
            - has header params
                - has one param per value col
            - has header refs
            - Dataframes are correct:
                - df_data has X by N shape
                    - Columns contain the required vars...
                - df_qc has X by Y shape
                - df_var has X by Y shape
                - Y + N == all columns?
            - Is valid ODV and/or BioODV
        '''
        good_file = False

        if len(self.params) > 0:
            logging.debug(f'Has {len(self.params)} params')
            good_file = True
        else:
            logging.warning('WARNING: No params parsed...')
            good_file = False

        if len(self.refs) > 0:
            logging.debug(f'Has {len(self.refs)} refs.')
            good_file = True
        else:
            logging.warning('WARNING: No refs parsed...')
            good_file = False

        if self.df_qc.shape == self.df_var.shape:
            logging.debug('Good file shape')
            good_file = True
        else:
            logging.warning('WARNING: Bad qc/var dimensions')
            good_file = False

        if self.valid_odv:
            good_file = True
        else:
            logging.warning('WARNING: Non-valid ODV file')
            good_file = False

        return good_file

    def parse_header(self):
        '''
        Parse the text header (the stuff filled with '//' cruft)
        into something readable
        '''

        index_sdn_ini = self.odv_header.find('//SDN_parameter_mapping')
        index_sdn_end = self.odv_header.find('//', index_sdn_ini + 2)
        ref_search = self.odv_header[index_sdn_ini:index_sdn_end + 2]
        refstr, paramstr = self.odv_header.split(ref_search)
        # refstr, paramstr = self.odv_header.split('//SDN_parameter_mapping\n//')
        refs = refstr.split('\n//')
        params = paramstr.split('\n//')

        self.refs = self.parse_psuedo_refs(refs)
        self.params = self.parse_psuedo_params(params)

        return

    def parse_psuedo_refs(self, xml_lines):
        '''
        Parse the pseudo xml from the headers into something meaningful.
        Return as a dict?
        '''
        parsed_list = []
        for line in xml_lines:
            if (line == '//') or (line == '\n'):
                # Empty lines of the pseudo xml header
                continue
            else:
                try:
                    line = line.lstrip('//')
                    soup = BeautifulSoup(line, "html.parser")
                    soup_dict = xmltodict.parse(str(soup)).get('sdn_reference')
                    parsed_list.append(soup_dict)
                except Exception as err:
                    # Possible Comment
                    self.comments.append(line)
                    logging.debug('--Problem Parsing Refs--')
                    logging.debug(err)
                    logging.debug(line)
        return parsed_list

    def parse_psuedo_params(self, xml_lines):
        '''
        Parse the pseudo xml from the headers into something meaningful.
        Return as a dict?
        '''
        parsed_list = []
        for line in xml_lines:
            try:
                line = line.lstrip('//')
                soup = BeautifulSoup(line, "html.parser")
                soup_dict = xmltodict.parse('<root>' + str(soup) + '</root>')['root']
                parsed_list.append(soup_dict)
            except Exception as err:
                logging.warning('--Problem Parsing Params--')
                logging.warning(err)
                logging.warning(line)
        return parsed_list

    def extract_chl_as_row_dict(self):
        fout = open('/mnt/c/DATA_LUIS/OCTAC_WORK/MATCH-UPS_ANALYSIS_2024/BAL/var_names.txt', 'a')
        col_names = self.odv_df.columns.tolist()
        index_chla = -1
        for index, name in enumerate(col_names):
            if name == 'CPHLSEP1 [Milligrams per cubic metre]':
                index_chla = index
                break
            if name.lower().find('chl') > 0:
                index_chla = index
                break

        if index_chla == -1:
            print(f'[WARNING]Chlorophyll value is not available')
            for name in col_names:
                if name.lower().find('chl') >= 0:
                    print(f'[WARNING] Could be chorophyll column: {name} ????????????????????????????')
                    fout.write('\n')
                    fout.write(name)
            return None

        # chla_names = ['CPHLSEP1 [Milligrams per cubic metre]','Chlorophyll a [mg/m3]','Chlorophyll-a [mg/m^3]','CHLA [mg/m^3]']
        depth_names = ['ADEPZZ01 [Metres]', 'depth [m]']
        depth_name = None
        for name in depth_names:
            if name in col_names:
                depth_name = name
                break

        if depth_name is None:
            print(f'[WARNING] Depth is not defined')
            if name.lower().find('depth') >= 0 and name != 'Bot. Depth [m]':
                print(f'[WARNING] Could be depth column: {name} ????????????????????????????')
                fout.write('\n')
                fout.write(name)
            return None
        fout.close()

        row = {x: self.odv_df.loc[0].at[x] for x in self.mandatory_columns}
        # try:
        #     #index_chla = self.odv_df.columns.tolist().index('CPHLSEP1 [Milligrams per cubic metre]')
        #     index_chla = self.odv_df.columns.tolist().index(chla_name)
        # except:
        #     print('[WARNING] CPHLSEP1 [Milligrams per cubic metre] is not available')
        #     try:
        #         index_chla = self.odv_df.columns.tolist().index('Chlorophyll a [mg/m3]')
        #     except:
        #         print('[WARNING] Chlorophyll a [mg/m3] is not available')
        #         return None

        time_stamp_format = self.get_time_stamp_format_python(self.mandatory_columns[-1])
        index_chla_qf = index_chla + 1

        ##index_zero-DEPRECATED
        # index_zero = self.odv_df.index[self.odv_df.loc[:,'ADEPZZ01 [Metres]']==0.0]
        # index_zero = self.odv_df.index[self.odv_df.loc[:, depth_name] == 0.0]
        # if len(index_zero) == 0:
        #     print(f'[WARNING] No chl-a values were set to a depth of 0 metres')
        #     return None
        # index_zero = int(index_zero[0])

        ##chloroplylll data with minimum depth


        depth_valid = self.odv_df.loc[self.odv_df[col_names[index_chla]]>0,depth_name]
        min_depth = np.min(depth_valid)
        index_zero = self.odv_df.index[self.odv_df.loc[:, depth_name] == min_depth]
        if len(index_zero) == 0:
            print(f'[WARNING] No chl-a values were set to a depth of 0 metres')
            return None
        index_zero = int(index_zero[0])
        chl_min_depth = self.odv_df.loc[self.odv_df[depth_name]==min_depth,col_names[index_chla]]
        chl_min_depth = float(np.array(chl_min_depth)[0])


        # imin = depth_valid.iloc[amin].index
        # print(len(depth_valid),'-->',amin,'==>',imin)
        # print(depth_valid)
        # print(amin,'--->',depth_valid.loc[amin])

        row['chl-a'] = self.odv_df.iloc[index_zero].iat[index_chla]
        print(min_depth,chl_min_depth,row['chl-a'])
        row['depth-ref_chl-a'] = min_depth
        row['QF_chl-a'] = self.odv_df.iloc[index_zero].iat[index_chla_qf]


        if time_stamp_format is not None and time_stamp_format != 'NODEF':
            time_here = dt.strptime(row[self.mandatory_columns[-1]], time_stamp_format)
            row['Datetime[UTC]'] = time_here.strftime('%Y-%m-%dT%H:%M:%S')
        else:
            row['Datetime[UTC]'] = row[self.mandatory_columns[-1]]
        del row[self.mandatory_columns[-1]]
        return row

        # print(depth,type(depth))

    def get_time_stamp_format_python(self, tsformat):
        if tsformat == 'YYYY-MM-DDThh:mm:ss' or tsformat == 'yyyy-mm-ddThh:mm:ss':
            pyformat = None
        elif tsformat == 'YYYY-MM-DDThh:mm:ss.sss' or tsformat == 'yyyy-mm-ddThh:mm:ss.sss':
            pyformat = '%Y-%m-%dT%H:%M:%S.%f'
        else:
            print(f'[WARNING] Review time format {tsformat}')
            pyformat = 'NODEF'
        return pyformat

    def save_to_csv_file(self, file_out):
        self.odv_df.to_csv(file_out, sep=';')
