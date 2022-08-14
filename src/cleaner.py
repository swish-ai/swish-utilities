from dataclasses import dataclass
import os
import json
import re
from time import time
import click
import pandas as pd
from hashlib import sha256
from pandas import read_csv, DataFrame
from cli_util import DipException
from src import settings

from flashtext import KeywordProcessor

split_compiled = re.compile(r"\n|\s")

WORDS_REGEX = r'([^(\s:)])+'
MA_MARK = '<#MASKING_ERROR>'
USER_CUSTOM = '<#USER_CUSTOM>'

MASK: int = 2
ANONYMIZE: int = 1
DROP: int = 3
FILTER_BY_WHITE_LIST = 4

def anonymize(x):
    s = sha256(str(x).encode('utf-8')).hexdigest()
    return f'<swish-anonymized-{s}>'


class UnsupportedFile(Exception):
    """Raised when CustomUserFile selected unsupported type"""
    pass


class CorruptedFile(Exception):
    """Raised when CustomUserFile selected unsupported type"""
    pass


class CustomUserFile:
    def __init__(self, filename, encodings=None):

        if encodings is None:
            encodings =  ['utf-8', 'latin-1', 'utf-8-sig']

        self.filename = filename

        self.ext = os.path.split(self.filename)[-1].split(".")[-1]
        self.data: list = []

        if self.ext == 'json':
            self.__load_json(encodings)
        elif self.ext == 'txt':
            self.__load_txt(encodings)
        elif self.ext == 'csv':
            self.__load_csv(encodings)
        else:
            raise UnsupportedFile("Unsupported file extension, file should be one of (txt,json,csv)")

    def __load_csv(self, encodings):
        try:

            for enc in encodings:
                try:
                    data = read_csv(self.filename, encoding=enc)
                except Exception as e:
                    click.echo(click.style(f"Failed to read file {self.filename}" +
                                   f" using encoding {enc}. Error: {e}", fg="yellow"))
            if data is None:
                click.echo(click.style(f"Failed to read file: {self.filename}", fg="red"))
                return

            if "user_custom_tokens" in data.columns:
                data = data["user_custom_tokens"].values.tolist()
            elif "user_defined_tokens" in data.columns:
                data = data["user_defined_tokens"].values.tolist()

            token_list = set()
            for column in data:
                token_list.update(data[column].dropna().to_list())

            token_list = list(token_list)
            data = list(filter(None, token_list))

            data = " ".join(data)
            self.finalize(data)
        except Exception:
            self.data = None

    def __load_txt(self, encodings):
        try:
            for enc in encodings:
                try:
                    with open(self.filename, "r", encoding=enc) as f:
                        data = f.read()
                except Exception as e:
                    click.echo(click.style(f"Failed to read file {self.filename}" +
                                   f" using encoding {enc}. Error: {e}", fg="yellow"))
            if data is None:
                click.echo(click.style(f"Failed to open file: {self.filename}", fg="red"))
                return

            self.finalize(data)

        except Exception:
            self.data = None

    def __load_json(self, encodings):
        try:
            for enc in encodings:
                try:
                    with open(self.filename, "r", encoding=enc) as f:
                        data = json.load(f, encoding=enc)
                except Exception as e:
                    click.echo(click.style(f"Failed to read file {self.filename}" +
                                   f" using encoding {enc}. Error: {e}", fg="yellow"))
            if data is None:
                click.echo(click.style(f"Failed to parse file: {self.filename}", fg="red"))
                return

            if "user_custom_tokens" in data:
                data = data["user_custom_tokens"]
            elif "user_defined_tokens" in data:
                data = data["user_defined_tokens"]

            token_list = set()
            if 'records' in data:
                df = DataFrame(data['records'])
            else:
                df = DataFrame(data)

            for column in df:
                token_list.update(df[column].to_list())

            token_list = list(token_list)
            data = list(filter(None, token_list))
            data = " ".join(data)
            self.finalize(data)

        except Exception:
            self.data = None

    def finalize(self, data):
        self.data = split_compiled.split(data)
        self.data = [x.strip() for x in self.data]
        self.data = list(filter(lambda x: x not in ('', None, '\n', '\r', '\t'), self.data))
        self.data = [x.strip() for x in self.data]

        if len(self.data) == 0:
            self.data = None


class PatternsMixin:
    def __init__(self, patterns):
        self.user_patterns = self.create_user_patterns(patterns)
        self._phone = re.compile(
            r'\+[0-9]{5,}|\+[0-9\.\ (\)-]{6,25}|\+[0-9(\)-]{1,6}[\ \.][0-9(\)\ \.-]{4,}|'
            r'\d{3}-\d{2}-\d{5,7}|'
            r'\+*\d{1,10}\ +\d{1,10}\S*|'
            r'\+\d{2,4} \d{2,5} \d{2,7} \(?\d{2,4}\)?|'
            r'\+\d{2,4} \d{1,5} \d{2,7} \d{1,7} (\d{1,7})?|'
            r'\+\d{2,4} \d{1,5} \d{2,7}( \d{1,7})?( \d{1,7})?|'
            r'\+?\(?\+?\d{1,3}\)?[-\s]\d{1,3}[-\s]\d{1,4}|'
            r'\+?\(?\+?\d{1,3}\)?[-\s]\d{1,3}[-\s]\d{1,4}[-\s]\d{1,4}|'
            r'\+\d{2,4} \(?\d{1,4}\)? \d{1,4}|'
            r'\d{2,4}[-\s]\d{2,4}[-\s]\d{2,4}([-\s]\d{2,4})?')

        self._url = re.compile(r"(http[s]?://(www\.)?|www\.)\S+", re.IGNORECASE)

        self._cc = re.compile(r'\d{3,4}-\d{3,4}-\d{3,4}-\d{3,4}')
        self._ip = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')
        self._email = re.compile(r'[\w\-.]+@[\w\-.]+', re.UNICODE)
        self._special = re.compile(r'[\x00-\x1f]', re.UNICODE)
        self._numbers = re.compile(r'\b\d{6,}\b')
        self._space = re.compile(r'\s+')
        self._ssn = re.compile(r'\b([a-z]*)(\d{3}[-_\.]\d{2}[-_\.]\d{4})([a-z]*)\b')

    def create_user_patterns(self, patterns):
        res = []
        if not patterns:
            return res
        for pattern in patterns:
            parts = pattern.rsplit(":", 1)
            ptrn = re.compile(parts[0])
            if len(parts) == 2:
                res.append((ptrn, parts[1]))
            else:
                res.append((ptrn, USER_CUSTOM))
        return res


class TextCleaner(PatternsMixin):

    def __init__(self, custom_tokens_filename_list: None, 
                important_token_file=None,
                encodings=None, patterns=None,
                preprocess_patterns=False):
        """
        Cleaner Class
        compiling regexes and custom tokens file.
        """
        super().__init__(patterns)
        
        self._custom_tokens_chunk = None
        self._flashtext_names = KeywordProcessor(case_sensitive=False)
        self.preprocess_patterns=preprocess_patterns

        self.__words = re.compile(WORDS_REGEX)

        self.__custom_tokens = None
        self.custom_tokens_list = set()
        self.important_tokens_list = set()
        self.set_important_tokens(important_token_file)
        self.set_custom_tokens(custom_tokens_filename_list, encodings=encodings)

    def set_important_tokens(self, important_token_file):

        try:
            if important_token_file is not None and important_token_file.data is not None:
                important_tokens = important_token_file.data
                self.important_tokens_list = set(important_tokens)

        except Exception as error:
            print("Error while extracting important tokens. Info: ", error)

    def set_custom_tokens(self, custom_tokens_filename_list=None, encodings = None):

        for file in custom_tokens_filename_list:
            if file:
                custom_tokens = CustomUserFile(filename=file, encodings=encodings).data
                if custom_tokens:
                    custom_tokens = list(dict.fromkeys(custom_tokens))
                    custom_tokens = re.split(r"[\s\.\-]", ' '.join(custom_tokens))
                    custom_tokens = set(map(lambda x: x.lower().strip(), custom_tokens))
                    self.custom_tokens_list.update(custom_tokens)

                print(f"Custom Token Loaded: Unique Count: {self.custom_tokens_list.__len__()}")
            else:
                self.__custom_tokens = None
                print("Custom Token Errors")

        self.custom_tokens_list -= self.important_tokens_list
        self.custom_tokens_list = list(self.custom_tokens_list)

        for name in self.custom_tokens_list:
            self._flashtext_names.add_keyword(name, " <#User> ")

    def get_custom_tokens(self):
        return self.custom_tokens_list

    def __replace_confident(self, match):
        try:
            src = match.group()
            x = src.strip()
            if self._special.search(x):
                return ''
            if self._ssn.search(x):
                return '\1 <#SSN> \3'
            if self._ip.search(x):
                return ' <#I> '
            if self._url.search(x):
                return ' <#U> '
            if self._cc.search(x):
                return ' <#CC> '
            if self._email.search(x):
                return ' <#M> '
            if self._phone.search(x):
                return ' <#P> '
            # if self.__catalog.search(x):
            #     return ' <#CG> '
            if self._numbers.search(x):
                return ' <#> '
            if self._space.search(x):
                return ' '
            if not self.preprocess_patterns:
                for pattern, replace in self.user_patterns:
                    if pattern.search(x):
                        return pattern.sub(replace, x)
            return src
        except Exception as e:
            click.echo(click.style(f"There was an Error while masking {x} {e}", fg="red"))
            return '<MASK_PROBLEM>'

    def clean_custom_tokens_chunk(self, x, no_clean=False):
        x = str(x)
        try:
            if no_clean:
                return x
            if self.preprocess_patterns:
                for pattern, replace in self.user_patterns:
                    x = pattern.sub(replace, x)

            #phone can contains several words, so it should be done separately
            x = self._phone.sub('<#P>', x)
            x = self.__words.sub(self.__replace_confident, x)
            
            if self.custom_tokens_list:
                x = self._flashtext_names.replace_keywords(x).strip()

            return x.strip()
        except Exception as e:
            click.echo(click.style(f"Masking Error {e} for entry {x} returning {MA_MARK}", fg="red"))
            return MA_MARK

    def transform(self, data: list) -> list:
        try:
            return list(map(lambda x: self.clean_custom_tokens_chunk(x), data))
        except Exception as e:
            click.echo(click.style(f"There was an Error while masking {e}", fg="red"))
        return data

    def __cond_masker(self, item):
        d, method = item
        if method == MASK:
            return self.clean_custom_tokens_chunk(d)
        if method == ANONYMIZE:
            return anonymize(d)
        if method == DROP:
            return None
        return d

    def condition_clean(self, df, cond_methos, column):
        data = []
        for index, row in df.iterrows():
            method = None
            for xx in cond_methos:
                cond_metho:MethodCondition = xx
                if row.get(cond_metho.col_name) == cond_metho.val:
                    method = cond_metho.method
                    break
            data.append((row[column], method))

        return list(map(self.__cond_masker, data))

    @staticmethod
    def anonymize_value(x):
        try:
            return anonymize(x)
        except Exception as e:
            click.echo(click.style(f"There was an Error while anonymizing {x}", fg="red"))
        return 'anonymizing_error'

    def anonymize(self, data: list, no_clean=None) -> list:
        if no_clean is None:
            return list(map(lambda x: TextCleaner.anonymize_value(x) if x and not pd.isna(x) and x != 'nan' else x, data))
        try:
            return list(map(lambda x: TextCleaner.anonymize_value(x[1]) if x[1] and not pd.isna(x[1]) and x[1] != 'nan' and not no_clean[x[0]] else x[1], enumerate(data)))
        except Exception as e:
            click.echo(click.style(f"There was an Error while anonymizing {e}", fg="red"))

        return data

    def transform_with_condition(self, data, no_clean=None):
        if no_clean is None:
            return self.transform(data)

        return [self.clean_custom_tokens_chunk(x, no_clean[i]) for i, x in enumerate(data)]

    def is_custom_loaded(self) -> bool:
        return bool(self.__custom_tokens)

    @DeprecationWarning
    def old_transform(self, x):
        x = str(x)
        x = self._special.sub('', x)
        x = self._cc.sub(lambda m: ' <#CC> ', x)
        x = self._phone.sub(' <#P> ', x)
        x = self._email.sub(' <#M> ', x)
        x = self._url.sub(' <#U> ', x)
        # x = self.__catalog.sub(' <#CG> ', x)
        x = self._ip.sub(' <#I> ', x)
        x = self._numbers.sub(lambda m: ' <#> ', x)
        x = self._space.sub(' ', x)
        x = self._ssn.sub('\1 <#SSN> \3', x)

        return x.strip()

    @DeprecationWarning
    def clean_custom_tokens(self, x):
        self.__custom_tokens = re.compile(r"\b(" + r"|".join(self.custom_tokens_list) + r")\b", re.IGNORECASE)
        x = str(x)
        x = self.__custom_tokens.sub(' <#User> ', x)
        x = self._space.sub(' ', x)
        return x.strip()

@dataclass
class MethodCondition:
    method:float
    col_name:str
    val:object


class Masker:
    def __init__(self, cleaner, params, mapping_file, custom_tokens_filename_list, anonymize_value, 
                 mask_value, drop_value):
        self.cleaner: TextCleaner = cleaner
        self.mapping_file = self.__update_auto(mapping_file)
        self.custom_tokens_filename_list = custom_tokens_filename_list
        self.methods = {'ANONYMIZE': anonymize_value,
                        'MASK': mask_value,
                        'DROP': drop_value}
        self.params = params
    
    def __update_auto(self, mapping_file):
        df = mapping_file.data
        auto_indices = []
        res = []
        for index, row in df.iterrows():
            if str(row["method"]).lower() == 'auto':
                auto_indices.append(index)
            else:
                res.append(f'fieldname={row["column"]};{row["method"]}|Field Name={row["column"]};{row["method"]}')

        auto_method = '|'.join(res)
        for index in auto_indices:
            df.at[index,'method'] = auto_method
        return mapping_file

    def __call__(self, items, no_pd=False, no_output_json=False):
        if not no_pd and not items:
            return items

        mapping_file = self.mapping_file
        cleaner = self.cleaner
        if no_pd:
            output_data = items
        else:
            output_data = pd.json_normalize(items)
        methods = self.methods

        output_data = self.__filter_rows(output_data, mapping_file)

        for column in output_data:
            self.__process_col(output_data, mapping_file, column, methods, cleaner)

        if no_output_json:
            return output_data

        return output_data.to_dict(orient="records")

    def __filter_rows(self, input_data, mapping_file):
        if mapping_file.filename != '' and \
                mapping_file.data is not None and \
                'column' in mapping_file.data:
            for column in input_data:
                
                if column in mapping_file.data['column'].to_list() and \
                    mapping_file.data[mapping_file.data['column'] == column]['method'].item() is not None:

                    data = mapping_file.data[mapping_file.data['column'] == column]
                    
                    method = data['method'].item()
                    condition = data['condition'].item()
                    if method == FILTER_BY_WHITE_LIST:
                        values = condition.split(' ')
                        cond = None
                        for i, val in enumerate(values):
                            if i == 0:
                                cond = (input_data[column] == values[i])
                            else:
                                cond = cond | (input_data[column] == values[i])
                        input_data = input_data[cond]
                input_data[column] = input_data[column].apply(lambda x: int(x) if type(x) == float and int(x) == x else x)
        return input_data

    def __get_condition_method(self, m, col):
        if not m:
            raise DipException(f'Bad conditional method for column {col}')
        res = []
        parts = m.split('|')
        for part in parts:
            if '=' in part:
                key, val = part.split('=')
                f, method = val.split(';')
                res.append(MethodCondition(float(method), key, f))
        return res

    def __process_col(self, output_data, mapping_file, column, methods, cleaner):
        method = None
        condition = None
        if mapping_file.filename != '' and \
                mapping_file.data is not None and \
               'column' in mapping_file.data and \
                column in mapping_file.data['column'].to_list() and \
                mapping_file.data[mapping_file.data['column'] == column]['method'].item() is not None:

            data = mapping_file.data[mapping_file.data['column'] == column]
            method = mapping_file.data[mapping_file.data['column'] == column]['method'].item()
            if method != FILTER_BY_WHITE_LIST and 'condition' in data:
                condition = data['condition'].item()

        cond_method = None
        if method is not None:
            try:
                method = float(method)
            except:
                cond_method = self.__get_condition_method(method, column)

        conditions = []
        if condition and not pd.isna(condition) and condition != 'nan':
            parts = [p.strip() for p in condition.split('|') if p.strip()]
            for part in parts:
                conditions.append(part.split('='))
        no_clean = None
        if conditions:
            all_column = output_data[column].values
            # if condition provided, set default to not applying it (no_clean) until we found that condition is truly
            no_clean = [True] * len(all_column)
            for condition in conditions:
                c_column, val = condition
                val = str(val)
                c_data = output_data[c_column].values
                for i, c in enumerate(c_data):
                    if str(c) == val:
                        no_clean[i] = False

        if self.params.white_list and column not in self.params.white_list:
            output_data.drop(column, axis=1, inplace=True)
            return

        if cond_method:
            output_data[column] = cleaner.condition_clean(output_data, cond_method, column)
        else:
            if 'MASK' in methods and method == methods['MASK']:
                # output_data[column] = output_data[column].fillna('')
                output_data[column] = cleaner.transform_with_condition(output_data[column].values, no_clean)
            if 'ANONYMIZE' in methods and method == methods['ANONYMIZE']:
                output_data[column] = cleaner.anonymize(output_data[column].values, no_clean)
            if 'DROP' in methods and method == methods['DROP']:
                output_data.drop(column, axis=1, inplace=True)


class PIIScanner(PatternsMixin):
    def __init__(self, input_file, params, app_settings):
        super().__init__(None)
        self.input_file = input_file
        self.params = params
        self.app_settings = app_settings

    def proccess(self):
        click.echo(f'Starting file scanning {self.input_file.filename}')
        f0 = time()
        input_file = self.input_file
        params = self.params
        if input_file.data is not None:
            data = input_file.data
            if not input_file.chunked:
                data = [data]
            for chunk in data:
                chunk.fillna(chunk.dtypes.replace({'float64': 0.0, 'O': 'NULL'}), downcast='infer', inplace=True)
                output_data = self.__check(chunk, no_pd=True, no_output_json=True)
        else:
            click.echo(click.style(f"File {input_file.filename} can not be processed", fg="red"))

    def __check(self, items, no_pd=False, no_output_json=False):
        if not no_pd and not items:
            return items

        if no_pd:
            output_data = items
        else:
            output_data = pd.json_normalize(items)

        for column in output_data.columns:
            data = output_data[column].values
            for d in data:
                print(d)

        if no_output_json:
            return output_data

        return output_data.to_dict(orient="records")
