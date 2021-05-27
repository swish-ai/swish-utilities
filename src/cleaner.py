import os
import json
import re

from pandas import read_csv, DataFrame
from src import settings

from flashtext import KeywordProcessor

split_compiled = re.compile(r"\n|\s|\t")
special_characters_compiled = re.compile(r"\.|\,")


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
            try:
                try:
                    with open(self.filename, "r", encoding='utf-8') as f:
                        data = json.load(f)
                except Exception as e:
                    try:
                        with open(self.filename, "r", encoding='latin-1') as f:
                            data = json.load(f)

                    except Exception as e:
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

            except Exception as e:
                self.data = None

        elif self.ext == 'txt':
            try:
                try:
                    with open(self.filename, "r", encoding='utf-8') as f:
                        data = f.read()

                except Exception as e:
                    with open(self.filename, "r", encoding='latin-1') as f:
                        data = f.read()

                self.finalize(data)

            except Exception as e:
                self.data = None

        elif self.ext == 'csv':
            try:
                try:
                    data = read_csv(self.filename, encoding='utf-8')
                except Exception as e:
                    data = read_csv(self.filename, encoding='latin-1')

                if "user_custom_tokens" in data.columns:
                    data = data["user_custom_tokens"].values.tolist()
                elif "user_defined_tokens" in data.columns:
                    data = data["user_defined_tokens"].values.tolist()

                token_list = set()
                for column in data:
                    token_list.update(data[column].dropna().to_list())

                token_list = list(token_list)
                data = list(filter(None,token_list))

                data = " ".join(data)
                self.finalize(data)
            except Exception as e:
                self.data = None

        else:
            raise UnsupportedFile("Unsupported file extension, file should be one of (txt,json,csv)")

    def finalize(self, data):
        self.data = split_compiled.split(data)
        self.data = [x.strip() for x in self.data]
        self.data = list(filter(lambda x: x not in ('', None, '\n', '\r', '\t'), self.data))
        self.data = [x.strip() for x in self.data]

        if len(self.data) == 0:
            self.data = None


class TextCleaner:

    def __init__(self, custom_tokens_filename_list: None, important_token_file = None):
        """
        Cleaner Class
        compiling regexes and custom tokens file.
        """
        # self._nproc = max(1, min(nproc, cpu_count()-1))
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
            print("Error while extracting important tokens. Info: ",error)

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

    def clean_custom_tokens_chunk(self, x):
        x = self._flashtext_names.replace_keywords(x).strip()
        x = self.__special.sub('', x)
        x = self.__ssn.sub('\1 <#SSN> \3', x)
        x = self.__ip.sub(' <#I> ', x)
        x = self.__url.sub(' <#U> ', x)
        x = self.__cc.sub(lambda m: ' <#CC> ', x)
        x = self.__email.sub(' <#M> ', x)
        x = self.__phone.sub(' <#P> ', x)
        x = self.__catalog.sub(' <#CG> ', x)
        x = self.__numbers.sub(lambda m: ' <#> ', x)
        x = self.__space.sub(' ', x)
        return x.strip()

    def transform(self, data: list) -> list:
        try:
            res = list(map(lambda x: self.clean_custom_tokens_chunk(x), data))
        except Exception as e:
            print("There was an Error in clean_custom_token_chunk ", e.__str__())

        return res

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

