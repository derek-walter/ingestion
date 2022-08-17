import ast
import gzip
import io
import json
import os
import re
import warnings
from collections import Counter
from gzip import GzipFile
from itertools import combinations
from math import ceil
from zipfile import ZipFile

import numpy as np
import openpyxl
import pandas as pd


def getFileManager(obj, sourceobj=None, file_encoding=None):
    """
    Return either the proper file manager or a dictionary of managers
    Parameters
    ----------
    sourceobj : file object
    file_encoding : file encoding
    Returns
    -------
    tuple of (values, count)
    """
    if (isinstance(obj, str) and ".zip" in obj) or isinstance(obj, ZipFile):
        return ZipManager(obj, sourceobj, file_encoding)
    elif (
        isinstance(obj, str) or isinstance(obj, io.IOBase) or isinstance(obj, GzipFile)
    ):
        return BaseFileManager(obj, sourceobj, file_encoding)
    elif isinstance(obj, list):
        return {
            subobj: getFileManager(
                subobj,
                sourceobj,
                file_encoding) for subobj in obj}
    else:
        raise NotImplementedError(
            "Unsupported feature passed as obj in getFileManager: {}".format(obj))


class FilesHelper:
    """
    Simple file categorizer
    Attributes
    ----------
    None
    Methods
    -------
    list_categories : List the file names by groups separated by _
    Examples
    --------
    >>> mc = MyClass()
    """
    def __init__(self, target, **kwargs):
        """Initialize and set target non-case-sensitive file stem or keywords, usually in form 'MyEnv' or 'MyEnv_Data'"""
        self.target = target

    @staticmethod
    def strip(item):
        return re.sub(r"[ \-,_]", "", item).lower()

    def list_categories(self, level=3):
        """List categories of found files grouped by keyword: level"""
        self.target_categories = sorted(
            list(set(["_".join(file.split("_")[:level]) for file in self.list_files()]))
        )
        return self.target_categories


class BaseFileManager:
    """
    Handles all files at the single file level, GZip, io.IOBase
    Attributes
    ----------
    None
    Methods
    -------
    None
    Examples
    --------
    >>> mc = MyClass()
    """
    def __init__(self, obj, sourceobj=None, file_encoding=None):
        self.sourceobj = sourceobj
        self.file_encoding = file_encoding
        if isinstance(obj, str):
            self.filename = obj.split("/")[-1]
        elif isinstance(obj, io.IOBase):
            self.filename = obj.name.split("/")[-1]
        elif isinstance(obj, GzipFile):
            self.filename = obj.filename
        else:
            raise NotImplementedError(
                "Unsupported feature passed to obj in FileManager: {}".format(obj))
        self.filetype = (
            components[-1]
            if len(components := self.filename.split(".")) == 2
            else ".".join(components[-2:])
        )
        self.fileobj = obj

    def open(self, mode="r"):
        """Attempt to open a file of many types, will attempt to import the artifact once if not found but will error if unsuccessful."""
        if self.sourceobj:
            if isinstance(self.sourceobj, str):
                self.sourceobj = ZipFile(self.sourceobj, mode)
            elif isinstance(self.sourceobj, ZipFile) and not self.sourceobj.fp:
                self.sourceobj = ZipFile(self.sourceobj.filename, mode)
            self.fileobj = self.sourceobj.open(self.filename, mode)
        elif isinstance(self.fileobj, str):
            if "xlsx" in self.filetype:
                _ = open(self.fileobj)
                _.close()
            else:
                if "gz" in self.filetype:
                    self.fileobj = gzip.open(
                        self.fileobj, "rt", encoding=self.file_encoding
                    )
                else:
                    self.fileobj = open(
                        self.fileobj, mode, encoding=self.file_encoding)
        elif isinstance(self.fileobj, io.IOBase):
            self.fileobj = open(
                self.filename, mode, encoding=self.file_encoding)
        else:
            raise NotImplementedError(
                f"Opening unknown filetype: {self.fileobj}")

    def _infer_delimiter(self):
        try:
            sample1, sample2 = self.fileobj.readline(), self.fileobj.readline()
            self.fileobj.seek(0, 0)
            sample1, sample2 = (
                sample1.decode() if isinstance(sample1, bytes) else sample1,
                sample2.decode() if isinstance(sample2, bytes) else sample2,
            )
            # TODO: Not multi-len delimiter safe. Example: matching ~| and
            # double matching \t for null cells ... (\t\t) -> (\t, \t) not easy

            def _delims(x):
                return re.findall("[\\t|,~;]", x)

            delims1, delims2 = map(_delims, (sample1, sample2))
            occurences1, occurences2 = map(
                lambda x: Counter(x).most_common(), (delims1, delims2)
            )
            print("Delims found: ", occurences1, occurences2)
            delimiter = sorted(
                list(set(occurences1).intersection(set(occurences2))),
                key=lambda x: x[1],
                reverse=True,
            ).pop()[0]
            return (delimiter, sample1, sample2, occurences1, occurences2)
        except Exception as e:
            self.fileobj.close()
            raise e

    def read_file_to_df(
        self,
        delimiter="infer",
        header="infer",
        names=None,
        engine=None,
        json_key=None,
        add_filename=True,
    ):
        """
        Return your values.
        Parameters
        ----------
        values : 1D list-like
        Returns
        -------
        tuple of (values, count)
        """
        try:
            self.open()
            if "csv" in self.filetype:
                df = pd.read_csv(
                    self.fileobj,
                    header=header,
                    engine=engine,
                    names=names,
                    encoding=self.file_encoding,
                )
            elif "tsv" in self.filetype:
                df = pd.read_csv(
                    self.fileobj,
                    delimiter="\t",
                    header=header,
                    engine=engine,
                    names=names,
                    encoding=self.file_encoding,
                )
            elif "xlsx" in self.filetype:
                df = pd.read_excel(
                    self.fileobj, header=header, engine="openpyxl")
            elif "xls" in self.filetype:
                df = pd.read_excel(self.fileobj.read())
            elif "txt" in self.filetype:
                if delimiter == "infer":
                    delimiter_pack = self._infer_delimiter()
                    self.inferred_delimiter_pack = delimiter_pack
                    inferred_delimiter = delimiter_pack[0]
                    df = pd.read_csv(
                        self.fileobj,
                        delimiter=inferred_delimiter,
                        header=header,
                        engine=engine,
                        names=names,
                        encoding=self.file_encoding,
                    )
                else:
                    df = pd.read_csv(
                        self.fileobj,
                        delimiter=delimiter,
                        header=header,
                        engine=engine,
                        names=names,
                        encoding=self.file_encoding,
                    )
            elif "json" in self.filetype:
                data = json.load(self.fileobj)
                if json_key:
                    json_to_df = data[json_key]
                    df = pd.DataFrame(json_to_df)
                else:
                    df = pd.read_json(data)
            else:
                raise NotImplementedError("File is in an incompatible format")
            if add_filename:
                df["source_filename"] = self.filename
            print("Imported ", df.shape, " records from ", self.filename)
            self.df = df
            return df
        except Exception as e:
            print("Error while attempting to read the file", self.filename)
            raise e
        finally:
            if not isinstance(self.fileobj, str):
                self.fileobj.close()


class FilesManager:
    """
    Description of my class
    Attributes
    ----------
    None
    Methods
    -------
    None
    Examples
    --------
    >>> mc = MyClass()
    """
    def __init__(self, filenames, sourceobj=None, file_encoding=None):
        self.filenames = filenames
        self.sourceobj = sourceobj
        self.filemanagers = {
            filename: getFileManager(
                filename, self.src_asset_id, self.sourceobj, file_encoding
            )
            for filename in self.filenames
        }


class ZipManager(FilesManager):
    """
    Description of my class
    Attributes
    ----------
    None
    Methods
    -------
    None
    Examples
    --------
    >>> mc = MyClass()
    """
    def __init__(self, obj, sourceobj=None, file_encoding=None):
        self.sourceobj = sourceobj
        if isinstance(obj, str):
            self.filename = obj
            if not self.sourceobj:
                self.fileobj = ZipFile(obj, "r")
            else:
                self.fileobj = ZipFile(self.sourceobj.open(obj, "r"), "r")
            self.filenames = self.fileobj.namelist()
        elif isinstance(obj, ZipFile):
            self.filename = obj.filename
            self.fileobj = obj
            self.filenames = self.fileobj.namelist()
        else:
            raise NotImplementedError("Unknown archive format: ", obj)
        super().__init__(self.filenames, self.fileobj, file_encoding)

    def extract_all(
        self,
        filenames=None,
        delimiter="infer",
        header=0,
        names=None,
        engine=None,
        json_key=None,
        add_filename=True,
    ):
        """
        Return your values.
        Parameters
        ----------
        values : 1D list-like
        Returns
        -------
        tuple of (values, count)
        """
        temp = pd.DataFrame()
        filenames = self.filemanagers.keys() if not filenames else filenames
        for filename in filenames:
            df = self.extract_one(
                filename,
                delimiter=delimiter,
                header=header,
                names=names,
                engine=engine,
                json_key=json_key,
                add_filename=add_filename,
            )
            temp = pd.concat((temp, df))
        self.fileobj.close()
        return temp

    def extract_one(
        self,
        filename=None,
        delimiter="infer",
        header=0,
        names=None,
        engine=None,
        json_key=None,
        add_filename=True,
    ):
        """
        Return your values.
        Parameters
        ----------
        values : 1D list-like
        Returns
        -------
        tuple of (values, count)
        """
        if not filename:
            filename = self.fileobj.namelist()[0]
        try:
            filemanager = self.filemanagers[filename]
        except KeyError:
            raise AttributeError("File not found in self.filemanagers")
        df = filemanager.read_file_to_df(
            delimiter=delimiter,
            header=header,
            names=names,
            engine=engine,
            json_key=json_key,
            add_filename=add_filename,
        )
        return df
