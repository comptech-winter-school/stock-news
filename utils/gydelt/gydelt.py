""" This module contains classes for accessing and processing data from GDELT """


__version__ = '1.0'
__author__ = 'Mrinal Jain'


import os
import string
from time import time
from datetime import datetime
import pandas as pd
import pandas_gbq as gbq
from google.cloud import bigquery
# country_state_list contains the 'countries' and 'US_states' list required for
# pre-processing the data
from . import country_state_list


class GetData(object):

    """ For collecting and storing the data """

    def read_from_file(self, path, seperator='\t',
                       parse_dates=False, encoding=None, header=0):
        """ Read data from a saved file into a pandas DataFrame

        **Parameters :**
                path (str) : ``(required)``
                        Path of the file to be read (**Do not forget** to add the file extension)

                separator (str) : ``default '\\t'``
                        Delimiter to use

                parse_dates (list) : ``default False``
                        eg - ['Date'] -> This column is parsed as a single Date column

                encoding (str) : ``default None``
                        Encoding to use for UTF when reading/writing (eg - ``'ISO-8859-1'``)

                header (int) : ``default 0``
                        Determines what row in the data should be considered for the Heading (Column names)

                        If no headers are needed, pass the value of 'header' as 'None' (without the quotes)

        **Returns :**
                pandas.DataFrame
                        A pandas DataFrame
        """

        try:
            data_frame = pd.read_csv(
                path,
                sep=seperator,
                parse_dates=parse_dates,
                encoding=encoding)
            return data_frame
        except UnicodeDecodeError:
            try:
                data_frame = pd.read_csv(
                    path,
                    sep=seperator,
                    parse_dates=parse_dates,
                    encoding='ISO-8859-1')
                return data_frame
            except UnicodeDecodeError:
                print(
                    '\nUnicodeDecodeError: \'UTF-8\' and \'ISO-8859-1\' cannot be used as encoding.\n')

    def fire_query(self, project_id, fields_required=['DATE', 'Themes', 'Locations', 'Persons', 'Organizations', 'V2Tone'],
                   is_search_criteria=False, get_stats=True, auth_file='', search_dict={}, limit=None, save_data=True):
        """ Fire a query on Google's BigQuery according to your search criteria and get the data in a pandas DataFrame

        **Parameters :**
                project_id (str) : ``(required)``
                        The ID of the project created on Google BigQuery, with which the query is to be executed

                fields_required (list) : ``default ['DATE', 'Themes', 'Locations', 'Persons', 'Organizations', 'V2Tone']``
                        The names of the columns to be extracted from the GKG Table on BigQuery

                is_search_criteria (boolean) : ``default False``
                        If True, then the user has to specify the Search Criteria (either through the console or by passing a dictionary)

                get_stats (boolean) : ``default True``
                        If True, then the amount of data that is to be processed will be displayed before executing the query (only if the location of 'auth_file' is given)

                auth_file (str) : ``default '' (empty)``
                        The path of the authorization file received from BigQuery.

                        .. note::
                                If the path of authorization file is provided, stats will be displayed before executing the query. If not, a message will be displayed and the user will be asked whether or not to proceeds

                search_dict (dict) : ``default {} (empty)``
                        Contains the Search Criteria in the following format -
                                Keys - Column from the GKG table, where the search is to be performed
                                Values - The keywords that are needed to be searched in the specific fields/columns

                        The values are divided into 3 parts -
                                Part 1 - similar to ``'Include ALL of'`` (boolean ``'and'`` is applied for each keyword)

                                Part 2 - similar to ``'Include ATLEAST ONE of'`` (boolean ``'or'`` is applied for each keyword)

                                Part 3 - similar to ``'Must NOT have ANY of'`` (boolean ``'not'`` is applied for each keyword)

                        Delimiter for the 3 parts is semi colon

                        Delimiter for keywords within each part is comma

                        .. note:: If No keywords is to be added in a certain part, leave it empty (BUT, DO NOT miss the semicolons)

                        Example - ``{'Persons': 'P1;P2,P3;P4', 'Organizations': ';O1,O2;'}``

                        >>> {'Locations': 'United States,China;;', 'Persons': ';;Donald Trump'}
                        # This would mean that the 'Locations' should have BOTH 'United States' and 'China' and 'Persons' should NOT have 'Donald Trump'

                        .. note:: The format while taking the input via console is also the same.

                                First, enter the required fields/columns (delimited by semi colon)

                                Then, for each field, enter the Keywords in the same format as mentioned for the search_dict

                        Example

                        >>> Enter the Field(s) : Persons;Organizations

                        >>> Include ALL of these in Persons: sundar pichai,narendra modi
                        >>> Include ATLEAST ONE of these in Persons: larry page,andrew ng
                        >>> Include NONE of these in Persons: donald trump

                        >>> Include ALL of these in Organizations: google
                        >>> Include ATLEAST ONE of these in Organizations: allen institute for artificial intelligence
                        >>> Include NONE of these in Organizations :

                        .. note::
                                If a dictionary is passed, it is **case-sensitive**. Therefore, give the values in the proper casing.

                limit (int) : ``default None``
                        The Maximum no. of rows to be returned from the result obtained by the Query.

                        .. note::
                                The max. size of the result is 128 MB.

                                If your query generates the data that exceeds 128 MB, you will need to specify the limit.

                save_data (boolean) : ``default True``
                        Will save the data in the current working directory

        **Returns :**
                pandas.DataFrame
                        A pandas DataFrame
        """

        flag = False
        is_console_input = True
        parse_date = False
        proceed = ''

        if 'DATE' in fields_required:
            fields_required[fields_required.index(
                'DATE')] = 'CAST(DATE AS STRING) as Date'
            parse_date = True

        if 'V2Tone' in fields_required:
            fields_required[fields_required.index(
                'V2Tone')] = 'V2Tone as ToneData'
        fields_req = ', '.join(fields_required)
        query = 'SELECT {} FROM [gdelt-bq:gdeltv2.gkg] WHERE '.format(
            fields_req)

        if (is_search_criteria):
            if(len(search_dict) != 0):
                is_console_input = False
            query = query + \
                self._create_query(
                    console_input=is_console_input,
                    search_dict=search_dict)
        else:
            query = query + 'True'

        if limit is not None:
            query = query + ' LIMIT {}'.format(limit)

        if get_stats == True:
            if auth_file != '':
                message = self._get_query_stats(
                    query=query, is_auth_file=True, auth_file=auth_file)
            else:
                message = self._get_query_stats(
                    query=query, is_auth_file=False)
            proceed = input(message + '\nProceed ?(Y/N)\n')

        if proceed == 'Y' or proceed == 'y' or get_stats == False:
            try:
                data_frame = gbq.read_gbq(query=query, project_id=project_id)
                if parse_date:
                    data_frame['Date'] = data_frame['Date'].apply(
                        lambda x: x[:8])
                    data_frame['Date'] = pd.to_datetime(
                        data_frame['Date'], format='%Y%m%d')
                if save_data:
                    self.save_data_frame(data_frame=data_frame)
                return data_frame
            except gbq.gbq.GenericGBQException:
                print(
                    '\nGenericGBQException: Quota exceeded - Your project exceeded quota for free query bytes scanned.\n')
                return pd.DataFrame()
        else:
            return pd.DataFrame()

    def save_data_frame(self, data_frame, path=None,
                        seperator='\t', index=False):
        """ Save a DataFrame (to a specified location/current working directory)

        **Parameters :**
                data_frame (pandas.DataFrame) :
                        The DataFrame that needs to be stored (in the specified format)

                path (str) : ``default None``
                        The path at which the file is to be stored

                        It should be of the following format - ``<dir>/<sub-dir>/.../<filename.csv>`` (recommended)

                        Mention the file name in the path itself (along with the extension)

                        .. note::
                                If no path is provided, the data frame will be saved in the current working directory.

                                Format of file name - ``Result(YYYY-MM-DD HH.MM.SS).csv``

                separator (str) : ``default '\\t'``
                        Delimiter to use

                index (boolean) : ``default False``
                        If True, then the index of the DataFrame will also be stored in the file

        **Returns :**
                None
                        A success message, along with the full path of the file.
        """

        if path is None:
            path = os.getcwd() + (r'\Result({}).csv'.format(datetime.now().strftime('%Y-%m-%d %H.%M.%S')))
        data_frame.to_csv(path, sep=seperator, index=index)
        print('Success: Data frame saved as - {}'.format(path))

    def _get_query_stats(self, query, is_auth_file, auth_file=''):
        """ To show the amount of data a query will process

        .. note::
                Used internally
        """
        if is_auth_file:
            client = bigquery.Client.from_service_account_json(auth_file)
            stats = client.query("""{};""".format(query))
            stats.dry_run = True
            stats.run()
            proceed = '\nThis query will process {} GB.'.format(
                round(float(stats.total_bytes_processed) / (1024 ** 3), 2))
        else:
            proceed = '\nNo stats available since the \'auth_file\' is not provided.'
        return proceed

    def _create_query(self, console_input, search_dict):
        """Converts the search criteria into an SQL Query

        .. note::
                Used Internally

                This method creates the query based on the search criteria provided in the method fire_query
        """

        condition = ''
        if(console_input):
            fields = input('Enter the Field(s) : ')
            fields = fields.split(';')
        else:
            fields = list(search_dict.keys())

        for field in fields:
            all_values = ''
            atleast_values = ''
            none_values = ''
            if (console_input):						# User will be prompted to give input via STDIN
                values_includeAll = input(
                    'Include ALL of these in {} : '.format(field))
                values_includeAtleast = input(
                    'Include ATLEAST ONE of these in {} : '.format(field))
                values_includeNone = input(
                    'Include NONE of these in {} : '.format(field))
                if field == 'Locations' or field == 'AllNames':
                    temp = values_includeAll.split(',')
                    for t in range(len(temp)):
                        temp[t] = string.capwords(temp[t], sep=' ')
                    values_includeAll = ','.join(temp)
                    temp = values_includeAtleast.split(',')
                    for t in range(len(temp)):
                        temp[t] = string.capwords(temp[t], sep=' ')
                    values_includeAtleast = ','.join(temp)
                    temp = values_includeNone.split(',')
                    for t in range(len(temp)):
                        temp[t] = string.capwords(temp[t], sep=' ')
                    values_includeNone = ','.join(temp)
                else:
                    values_includeAll = values_includeAll.lower()
                    values_includeAtleast = values_includeAtleast.lower()
                    values_includeNone = values_includeNone.lower()
                values = '{};{};{}'.format(
                    values_includeAll,
                    values_includeAtleast,
                    values_includeNone)
            else:
                # Otherwise, the given dict will be processed
                values = search_dict[field]
            values = values.split(';')

            for i in range(len(values)):
                values[i] = values[i].split(',')

            # For 'Include ALL of'
            for item in values[0]:
                if (item != ''):
                    all_values = all_values + \
                        '{} LIKE "%{}%"'.format(field, item)
                    if (values[0].index(item) != (len(values[0]) - 1)):
                        all_values = all_values + ' and '
                    else:
                        all_values = '({})'.format(all_values)
                else:
                    all_values = '(True)'

            for item in values[1]:											# For 'Include ATLEAST ONE of'
                if (item != ''):
                    atleast_values = atleast_values + \
                        '{} LIKE "%{}%"'.format(field, item)
                    if (values[1].index(item) != (len(values[1]) - 1)):
                        atleast_values = atleast_values + ' or '
                    else:
                        atleast_values = '({})'.format(atleast_values)
                else:
                    atleast_values = '(True)'

            for item in values[2]:											# For 'Must NOT have ANY of'
                if (item != ''):
                    none_values = none_values + \
                        '{} NOT LIKE "%{}%"'.format(field, item)
                    if (values[2].index(item) != (len(values[2]) - 1)):
                        none_values = none_values + ' and '
                    else:
                        none_values = '({})'.format(none_values)
                else:
                    none_values = '(True)'

            condition = condition + \
                '{} and {} and {}'.format(
                    all_values, atleast_values, none_values)
            if (fields.index(field) != (len(fields) - 1)
                ):					# Checking if it's the last condition or not
                condition = condition + ' and '

        return condition


class ProcessData(object):

    """ Contains wrappers to pre-process various fields of the data collected from GDELT """

    def __init__(self, data_frame, location='Locations', person='Persons',
                 organization='Organizations', tone='ToneData', theme='Themes'):
        """ Constructor of the class ``ProcessData``

        **Parameters :**
                data_frame (pandas.DataFrame) :
                        The DataFrame obtained from GDELT, which is to be processed

                location (str) : ``default 'Locations'``
                        The name of the field in the passed DataFrame that corresponds to ``Locations``

                person (str) : ``default 'Persons'``
                        The name of the field in the passed DataFrame that corresponds to ``Persons``

                organization (str) : ``default 'Organizations'``
                        The name of the field in the passed DataFrame that corresponds to ``Organizations``

                tone (str) : ``default 'ToneData'``
                        The name of the field in the passed DataFrame that corresponds to ``ToneData``

                theme(str) : ``default 'Themes'``
                        The name of the field in the passed DataFrame that corresponds to ``Themes``
        """

        self.data_frame = data_frame
        self.location = location
        self.person = person
        self.organization = organization
        self.tone = tone
        self.theme = theme

    def check_country_list(self):
        """ Returns those Locations (countries) which were not present in the countries list

        **Returns :**
                list
                        A list containing the locations for which there was no match in the default country list
        """

        missing = []
        loc = ''
        for locations in self.data_frame[self.location]:
            count = 0
            location = locations.split(';')
            for c in country_state_list.countries:
                for loc in location:
                    if (c[1] in loc):
                        count = 1
            if (count == 0):
                missing.append(loc)
        return missing

    def clean_locations(self, only_country=True, fillna='unknown'):
        """ Pre-process the 'Locations' column of the data (Extract either all details available, or just the Countries)

        **Parameters :**
                only_country (boolean) : ``default True``
                        If True, will keep only the country names for each row in the ``Locations`` column

                        If False, will keep whatever details available (city, state or country)

                fillna (str) : ``default 'unknown'``
                        To fill the Null values (``NaN``) with the specified value

        **Returns :**
                pandas.DataFrame
                        A pandas DataFrame (with additional fields for ``Countries`` and ``States``, if required)
        """

        self.data_frame[self.location].fillna(fillna, inplace=True)
        if 'Countries' not in list(self.data_frame.columns):
            self.data_frame.loc[:, 'Countries'] = self.data_frame[self.location].apply(
                lambda x: self._process_locations(row=x))
        if only_country:
            pass
        else:
            self.data_frame.loc[:, 'States'] = self.data_frame[self.location].apply(
                lambda x: self._process_locations_states(row=x))
        return self.data_frame

    def clean_persons(self, fillna='unknown', max_no_of_words=6):
        """ Filters out the ``Persons`` column of the data.

        Only those names are kept in which the no. of words are within a certain limit

        **Parameters :**
                fillna (str) : ``default 'unknown'``
                        To fill the Null values (``NaN``) with the specified value

                max_no_of_words (int) : ``default 6``
                        Removes all the names whose length is greater than this value from each record/row

        **Returns :**
                pandas.DataFrame
                        A pandas DataFrame (with updated ``'Persons'``)
        """

        self.data_frame[self.person].fillna(fillna, inplace=True)
        self.data_frame.loc[:, self.person] = self.data_frame[self.person].apply(
            lambda x: self._process_persons(x, max_words=max_no_of_words))
        return self.data_frame

    def clean_organizations(self, fillna='unknown'):
        """ Pre-processes the ``Organizations`` column. Removes certain invalid Organizations

        .. note::
                Some Countries (eg. United States) have been mistaken as individual Organizations

                This function removes those Organizations (which are actually Countries), from each record/row

        **Parameters :**
                fillna (str) : ``default 'unknown'``
                        To fill the Null values (``NaN``) with the specified value

        **Returns :**
                pandas.DataFrame
                        A pandas DataFrame (with updated ``Organizations``)
        """

        self.data_frame[self.organization].fillna(fillna, inplace=True)
        self.data_frame.loc[:, self.organization] = self.data_frame[self.organization].apply(
            lambda x: self._process_organizations(x))
        # If after processing the Organizations, a NaN value is created
        self.data_frame[self.organization].fillna(fillna, inplace=True)
        return self.data_frame

    def seperate_tones(self):
        """ Creates Separate columns for each value in ``ToneData``

        **Returns :**
                pandas.DataFrame
                        A pandas DataFrame

                .. note::
                        The ``ToneData`` column has 7 vaules, which are converted into seperate columns in the data frame.
                        The original ``ToneData`` remains intact
        """

        seperate = [
            'Tone',
            'Positive Score',
            'Negative Score',
            'Polarity',
            'Activity Reference Density',
            'Self/Group Reference Density',
            'Word Count']
        for i in range(len(seperate)):
            try:
                self.data_frame.loc[:, seperate[i]] = self.data_frame[self.tone].apply(
                    lambda x: float('{:.15f}'.format(float(x.split(',')[i]))))
            except IndexError:
                self.data_frame.loc[:, seperate[i]] = None
        return self.data_frame

    def clean_themes(self, fillna='unknown'):
        """ Fills the Null values (``NaN``) in the ``Themes`` column of data

        **Parameters :**
                fillna (str) : ``default 'unknown'``
                        To fill the Null values (``NaN``) with the specified value

        **Returns :**
                pandas.DataFrame
                        A pandas DataFrame (with Null values in ``Themes`` filled)
        """

        self.data_frame[self.theme].fillna(fillna, inplace=True)
        return self.data_frame

    def flat_column(self, columns=[], fillna='unknown'):
        """ The given list of columns are flattened (using one-hot encoding) and the resulting columns are added to the DataFrame

        **Parameters :**
                fillna (str) : default 'unknown'
                        To fill the Null values (``NaN``) with the specified value

        **Returns :**
                pandas.DataFrame
                        A pandas DataFrame

                .. note::
                        All the column names passed in the list ``columns`` are flattened (one-hot encoding is used).

                        The new data frame returned contains additional columns, which are the individual and unique values present in the respective columns which are required to be flattened.
        """

        start_time = time()

        if len(columns) == 0:
            print('No columns passed to flatten !')
            return self.data_frame
        else:
            for column in columns:
                self.data_frame[column].fillna(fillna, inplace=True)
                values = []
                for i in self.data_frame[column]:
                    values.extend(i.split(';'))

                values = list(set(values))
                if '' in values:
                    values.remove('')
                values.sort()

                for i in values:
                    self.data_frame.loc[:, i] = self.data_frame[column].apply(
                        lambda x: self._one_hot_encode(x, i))

            end_time = time()
            print(
                '\nTime taken for flattening the column(s) --> {:.2f} seconds'.format(end_time - start_time))

            return self.data_frame

    def pre_process(self):
        """ A wrapper functions that does all the pre-processig. (Except - flattening)

        **Returns :**
                pandas.DataFrame
                        A clean and processed pandas DataFrame
        """

        start_time = time()

        to_clean = list(self.data_frame.columns)
        if self.location in to_clean:
            self.data_frame = self.clean_locations()
        if self.person in to_clean:
            self.data_frame = self.clean_persons()
        if self.organization in to_clean:
            self.data_frame = self.clean_organizations()
        if self.tone in to_clean:
            self.data_frame = self.seperate_tones()
        if self.theme in to_clean:
            self.data_frame = self.clean_themes()

        end_time = time()
        print(
            '\nTime taken for pre-processing the data --> {:.2f} seconds'.format(end_time - start_time))

        return self.data_frame

    def save_data_frame(self, path=None, seperator='\t', index=False):
        """ Save a DataFrame (to a specified location/current working directory)

        **Parameters :**
                path (str) : ``default None``
                        The path at which the file is to be stored

                        It should be of the following format - ``<dir>/<sub-dir>/.../<filename.csv>`` (recommended)

                        Mention the file name in the path itself (along with the extension)

                        .. note::
                                If no path is provided, the data frame will be saved in the current working directory.

                                Format of file name - ``Result(YYYY-MM-DD HH.MM.SS).csv``

                separator (str) : ``default '\\t'``
                        Delimiter to use

                index (boolean) : ``default False``
                        If True, then the index of the DataFrame will also be stored in the file

        **Returns :**
                None
                        A success message, along with the full path of the file.
        """

        if path is None:
            path = os.getcwd() + (r'\Result({}).csv'.format(datetime.now().strftime('%Y-%m-%d %H.%M.%S')))
        self.data_frame.to_csv(path, sep=seperator, index=index)
        print('Success: Data frame saved as - {}'.format(path))

    def _process_persons(self, row, max_words):
        """ Returns only those names having a no. of words below a certain limit

        .. note::
                Used Internally
        """

        names = row.split(';')
        finalNames = []
        for name in names:
            if (len(name.split(' ')) <= max_words):
                finalNames.append(name)
        return ';'.join(finalNames)

    def _process_locations(self, row):
        """ Returns the Names of Countries from each row (delimited by semi-colon)

        .. note::
                Used Internally
        """

        temp = row.split(';')
        cleanList = []
        for c in country_state_list.countries:
            for t in temp:
                if (c[1] in t):
                    # This will only extract the Country name
                    cleanList.append(c[1])
        return ';'.join(set(cleanList))

    def _process_locations_states(self, row):
        """ Returns the States (of the US) from each row, if present (delimited by semi-colon)

        .. note::
                Used Internally
        """

        temp = row.split(';')
        stateList = []
        for t in temp:
            if 'United States' in t:
                for state in country_state_list.US_states:
                    if state[1] in t:
                        stateList.append(state[1])
        if len(stateList) == 0:
            return 'unknown'
        return ';'.join(set(stateList))

    def _process_organizations(self, row):
        """ Removes those organizations which have the exact same name as a country

        .. note::
                Used Internally
        """

        organizations = row.split(';')
        organizations = [org.lower() for org in organizations]
        for country in country_state_list.countries:
            if country[1].lower() in organizations:
                organizations.remove(country[1].lower())
        return ';'.join(organizations)

    def _one_hot_encode(self, row, val):
        """ Used for One-Hot Encoding

        Checks if the value passed is present in the current row.

        If Yes, then the value of that particular column, for that row is 1

        .. note::
                Used Internally
        """

        if val in row.split(';'):
            return 1
