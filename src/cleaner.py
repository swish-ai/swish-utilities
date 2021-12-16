import os
import json
import re
import click
import pandas as pd
from hashlib import sha224
from pandas import read_csv, DataFrame
from src import settings

from flashtext import KeywordProcessor

split_compiled = re.compile(r"\n|\s")

WORDS_REGEX = r'([A-Za-z-]|[\\u5D0-\\u05EA])+'


class UnsupportedFile(Exception):
    """Raised when CustomUserFile selected unsupported type"""
    pass


class CorruptedFile(Exception):
    """Raised when CustomUserFile selected unsupported type"""
    pass


class CustomUserFile:
    def __init__(self, filename):
        self.filename = filename

        self.ext = os.path.split(self.filename)[-1].split(".")[-1]
        self.data: list = []

        if self.ext == 'json':
            self.__load_json()
        elif self.ext == 'txt':
            self.__load_txt()
        elif self.ext == 'csv':
            self.__load_csv()
        else:
            raise UnsupportedFile("Unsupported file extension, file should be one of (txt,json,csv)")

    def __load_csv(self):
        try:
            try:
                data = read_csv(self.filename, encoding='utf-8')
            except Exception:
                data = read_csv(self.filename, encoding='latin-1')

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

    def __load_txt(self):
        try:
            try:
                with open(self.filename, "r", encoding='utf-8') as f:
                    data = f.read()

            except Exception:
                with open(self.filename, "r", encoding='latin-1') as f:
                    data = f.read()

            self.finalize(data)

        except Exception:
            self.data = None

    def __load_json(self):
        try:
            try:
                with open(self.filename, "r", encoding='utf-8') as f:
                    data = json.load(f)
            except Exception:
                try:
                    with open(self.filename, "r", encoding='latin-1') as f:
                        data = json.load(f)

                except Exception:
                    with open(self.filename, "r", encoding='utf-8-sig') as f:
                        data = json.load(f)

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


class TextCleaner:

    def __init__(self, custom_tokens_filename_list: None, important_token_file=None):
        """
        Cleaner Class
        compiling regexes and custom tokens file.
        """
        self._custom_tokens_chunk = None
        self._flashtext_names = KeywordProcessor(case_sensitive=False)

        self.__phone = re.compile(
            r'\d{3}-{2}-{5,7}|'
            r'\+\d{2,4} \d{2,5} \d{2,7} \(?\d{2,4}\)?|'
            r'\+\d{2,4} \d{1,5} \d{2,7} \d{1,7} (\d{1,7})?|'
            r'\+\d{2,4} \d{1,5} \d{2,7}( \d{1,7})?( \d{1,7})?|'
            r'\+?\(?\+?\d{1,3}\)?[-\s]\d{1,3}[-\s]\d{1,4}|'
            r'\+?\(?\+?\d{1,3}\)?[-\s]\d{1,3}[-\s]\d{1,4}[-\s]\d{1,4}|'
            r'\+\d{2,4} \(?\d{1,4}\)? \d{1,4}|'
            r'\d{2,4}[-\s]\d{2,4}[-\s]\d{2,4}([-\s]\d{2,4})?')

        self.__url = re.compile(r"(http[s]?://(www\.)?|www\.)\S+", re.IGNORECASE)
        self.__catalog = re.compile(r'\b(\d+[a-zA-Z]|[a-zA-Z]+\d)[\w\-\_\!\?\.\#\$\%\^\&\*\.\(\)\\\/]+\b')

        self.__cc = re.compile(r'\d{3,4}-\d{3,4}-\d{3,4}-\d{3,4}')
        self.__ip = re.compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')
        self.__email = re.compile(r'[\w\-.]+@[\w\-.]+', re.UNICODE)
        self.__special = re.compile(r'[\x00-\x1f]', re.UNICODE)
        self.__numbers = re.compile(r'\b\d{6,}\b')
        self.__space = re.compile(r'\s+')
        self.__ssn = re.compile(r'\b([a-z]*)(\d{3}[-_\.]\d{2}[-_\.]\d{4})([a-z]*)\b')

        self.__words = re.compile(WORDS_REGEX)

        self.__custom_tokens = None
        self.custom_tokens_list = set()
        self.important_tokens_list = set()
        self.set_important_tokens(important_token_file)
        self.set_custom_tokens(custom_tokens_filename_list)

    def set_important_tokens(self, important_token_file):

        try:
            if important_token_file is not None and important_token_file.data is not None:
                important_tokens = important_token_file.data
                self.important_tokens_list = set(important_tokens)

        except Exception as error:
            print("Error while extracting important tokens. Info: ", error)

    def set_custom_tokens(self, custom_tokens_filename_list=None):

        for file in custom_tokens_filename_list:
            if file:
                custom_tokens = CustomUserFile(filename=file).data
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
        x = match.group()
        if self.__special.match(x):
            return ''
        if self.__ssn.match(x):
            return '\1 <#SSN> \3'
        if self.__ip.match(x):
            return ' <#I> '
        if self.__url.match(x):
            return ' <#U> '
        if self.__cc.match(x):
            return ' <#CC> '
        if self.__email.match(x):
            return ' <#M> '
        if self.__phone.match(x):
            return ' <#P> '
        if self.__catalog.match(x):
            return ' <#CG> '
        if self.__numbers.match(x):
            return ' <#> '
        if self.__space.match(x):
            return ' '
        return x

    def clean_custom_tokens_chunk(self, x, no_clean=False):
        if no_clean:
            return x
        if self.custom_tokens_list:
            x = self._flashtext_names.replace_keywords(x).strip()
        x = self.__words.sub(self.__replace_confident, x)

        return x.strip()

    def transform(self, data: list) -> list:
        try:
            res = list(map(lambda x: self.clean_custom_tokens_chunk(x), data))
        except Exception as e:
            click.echo(click.style(f"There was an Error while masking {e}", fg="red"))
        return res

    def anonymize(self, data: list) -> list:
        try:
            res = list(map(lambda x: sha224(x.encode('utf-8')).hexdigest(), data))
        except Exception as e:
            click.echo(click.style(f"There was an Error while anonymizing {e}", fg="red"))

        return res

    def transform_with_condition(self, df, column, conditions):
        if not conditions:
            return self.transform(df[column].values.tolist())

        data = df[column].values.tolist()
        no_clean = [True] * len(data)
        for condition in conditions:
            c_column, val = condition
            val = str(val)
            c_data = df[c_column].values.tolist()
            for i, c in enumerate(c_data):
                if str(c) == val:
                    no_clean[i] = False
        return [self.clean_custom_tokens_chunk(x, no_clean[i]) for i, x in enumerate(data)]

    def is_custom_loaded(self) -> bool:
        return bool(self.__custom_tokens)

    @DeprecationWarning
    def old_transform(self, x):
        x = str(x)
        x = self.__special.sub('', x)
        x = self.__cc.sub(lambda m: ' <#CC> ', x)
        x = self.__phone.sub(' <#P> ', x)
        x = self.__email.sub(' <#M> ', x)
        x = self.__url.sub(' <#U> ', x)
        x = self.__catalog.sub(' <#CG> ', x)
        x = self.__ip.sub(' <#I> ', x)
        x = self.__numbers.sub(lambda m: ' <#> ', x)
        x = self.__space.sub(' ', x)
        x = self.__ssn.sub('\1 <#SSN> \3', x)

        return x.strip()

    @DeprecationWarning
    def clean_custom_tokens(self, x):
        self.__custom_tokens = re.compile(r"\b(" + r"|".join(self.custom_tokens_list) + r")\b", re.IGNORECASE)
        x = str(x)
        x = self.__custom_tokens.sub(' <#User> ', x)
        x = self.__space.sub(' ', x)
        return x.strip()


class Masker:
    def __init__(self, cleaner, mapping_file, custom_tokens_filename_list, anonymize_value, mask_value):
        self.cleaner: TextCleaner = cleaner
        self.mapping_file = mapping_file
        self.custom_tokens_filename_list = custom_tokens_filename_list
        self.methods = {'ANONYMIZE': anonymize_value,
                        'MASK': mask_value}

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
        for column in output_data:
            message = f'Column: {column} | Start", end=" | '
            print(message)

            self.__process_col(output_data, mapping_file, column, methods, cleaner)

        if no_output_json:
            return output_data

        return output_data.to_dict(orient="records")

    def __process_col(self, output_data, mapping_file, column, methods, cleaner):
        method = None
        condition = None
        if mapping_file.filename != '' and \
                mapping_file.data is not None and \
                column in mapping_file.data['column'].to_list() and \
                mapping_file.data[mapping_file.data['column'] == column]['method'].item() is not None:

            condition = mapping_file.data[mapping_file.data['column'] == column]['condition'].item()
            method = mapping_file.data[mapping_file.data['column'] == column]['method'].item()

        conditions = []
        if condition and not pd.isna(condition) and condition != 'nan':
            parts = [p.strip() for p in condition.split('|') if p.strip()]
            for part in parts:
                conditions.append(part.split('='))

        if 'MASK' in methods and method == methods['MASK']:
            output_data[column] = output_data[column].fillna('')
            output_data[column] = cleaner.transform_with_condition(output_data, column, conditions)
        if 'ANONYMIZE' in methods and method == methods['ANONYMIZE']:
            output_data[column] = cleaner.anonymize(output_data[column].values.tolist())
