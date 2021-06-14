"""Service models and factories."""
import re
import logging
from copy import copy
from typing import Generator, List, Any
from collections import OrderedDict
from enum import Enum

# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class FilterType(Enum):
    """Enum to define the different types of filters which can be applied to a record."""
    UNIQUE = 'UNIQUE'


# Model objects
class JSONManifest:
    """JSONManifest object.

    This objects as a container for a json document. JSONManifest instances,
    initialized with a python dictionary and a list of rules, will act as an
    iterator for the resulting combination of the two documents.

    The logic is simple: for every rule from the passed-in list of rules,
    if the `source` values matches the path for a value in the data, then
    the manifest will output the `target` along with the value.

    Parameters
    ----------
    data : dict{str:any}
        The data dictionary that the JSONManifest instance will wrap.
    rules : list[dict]
        A list of rules to apply to the ingested data.
        Rules must be a list of dictionaries, each with a `source` and `target`
        key to operate correctly.

    Attributes
    ----------
    data : dict{str:any}
        The ingested data.
    rules : list[dict]
        The ingested rules.
    items : dict{str:any}
        A dictionary where the keys are the target paths and the values are
        the values, after the transformation has occurred.

    """

    # Instance attributes
    @property
    def data(self) -> dict:
        """Return a copy of the internal read-only _data attributes."""
        return copy(self._data)

    @property
    def rules(self) -> list:
        """Return a copy of the internal read-only _rules attribute."""
        return copy(self._rules)

    @property
    def items(self) -> list:
        """Return a dictionary of the mapped data, per the given rules."""
        return dict(iter(self))

    @property
    def filters(self) -> list:
        """Return a dictionary of filter types and the paths at which to apply them"""
        return copy(self._filters)

    def __init__(self, data: dict = None, rules: list = None):
        data = {} if data is None else data
        rules = [] if rules is None else rules
        self._data, self._rules = data, rules
        self._filters = self.find_filters(rules)

        # Flatten source data for faster parsing
        self._fdata = dict(self.flatten(self._data))

    def __iter__(self):
        """Iterate on the rules and items, yielding only those which match."""

        target_track = {}
        for rule in self._rules:
            # Handle basic source-target mapping
            if 'source' in rule:
                for path, value in self._fdata.items():
                    if rule.get('source') == path:
                        yield rule.get('target'), value

            # Handle check_match boolean mapping
            if 'check_match' in rule:
                check_match_values= []
                for path, value in self._fdata.items():
                    # Build out list of path/value pairs for the given check_match rule
                    for candidate_path in rule.get('check_match'):
                        if candidate_path in path:
                            check_match_values.append((path.replace(candidate_path, ''), value))

                # For empty lists, skip this mapping rule
                if check_match_values:
                    yield rule.get('target'), \
                          len([t for t in (set(tuple(i) for i in check_match_values))]) == \
                          len(check_match_values)/2

            # Handle "iterate" rules to take care of lists of unknown sizes
            # Keeps Track of 2 Lists:
            #    (a) target_track : dictionary with target lists and what index we are on
            #    (b) source_track: : list to keep track of what index of the source list we are on
            if 'iterate' in rule:
                source_track = []
                iterate_rule = rule.get('iterate')
                for path, value in self._fdata.items():
                    for mapping in iterate_rule.get('mappings'):
                        if mapping.get('source') in path and \
                                iterate_rule.get('source_list') in path:
                            target_list = f"{iterate_rule.get('target_list')}"
                            target_track[target_list] = target_track.get(target_list, 0)

                            # Using regex, check the source list if we have looked at this index yet
                            modified_sl = str(iterate_rule.get('source_list')).\
                                replace('$.', r'\$\.')
                            source_list_regex = fr"({modified_sl}\[\d+\])"
                            match = re.findall(source_list_regex, path)
                            if len(match) > 0:
                                if match[0] not in source_track:
                                    if len(source_track) != 0:
                                        target_track[target_list] += 1
                                    source_track.append(match[0])

                            # Build the target path and return it with the value
                            target_path = \
                                f"{target_list}[{target_track[target_list]}]{mapping.get('target')}"
                            yield target_path, value

                # Increase the count for the given target list
                target_track[target_list] += 1

    # Static methods
    @staticmethod
    def flatten(data: dict) -> Generator:
        """Flatten the given dictionary to a list of paths and values.

        Parameters
        ----------
        data : dict{str:any}
            The data dictionary which should be flattened.

        Returns
        -------
        Generator
            Returns a generator, which when iterated on, will yield key-value
            pairs where the values are the individual values from the ingested
            data and the keys are the valid JSONPaths to those values.

        """

        def iter_child(cdata: Any, keys: List[str] = None):
            keys = [] if keys is None else keys

            if isinstance(cdata, dict):
                for key, value in cdata.items():
                    yield from iter_child(value, keys + [key])

            elif isinstance(cdata, list):
                for idx, value in enumerate(cdata):
                    key = f'{keys[-1]}[{str(idx)}]'
                    yield from iter_child(value, keys[:-1] + [key])

            else:
                yield '.'.join(keys), cdata

        yield from iter_child(data, ['$'])

    @staticmethod
    def find_filters(rules: dict):
        """Extract the filters from the list of rules.

            Filters are a custom mapping rule that allow you to groom the mapped
            values before returning the report. Supported filters are defined in
            the FilterType class.

        Parameters
        ----------
        rules : dict{str:any}
            The dictionary of all mapping rules.

        Returns
        -------
        dict{FilterType:any}
            Returns a data dictionary of filter types and the paths at which to apply them.

        """
        rules = {} if rules is None else rules
        filters = {}

        filter_unique = 'filter_unique'
        filters[FilterType.UNIQUE] = [r.get(filter_unique) for r in rules if filter_unique in r]

        return filters


# Factory objects
class JSONFactory:
    """JSONFactory object.

    This class acts as a factory ontop of JSONManifest objects to
    reconstitute all mapped values back into a valid JSON document, which is
    called the "Projection" or the "Projected JSON".

    Parameters
    ----------
    manifest : JSONManifest
        The JSONManifest object, which is a container for the rules and data
        that should be combined to create the projected JSON.

    Attributes
    ----------
    RE_PAT : re.Pattern
        A regex pattern which parses JSONPaths for queries.
    RE_IDX : dict{str:str}
        A dictionary which represents the group names of `RE_PAT`.

    """

    # Class attributes
    _vals = r"(?:['\"]\s*[\w\.\s-]+\s*['\"]|\d+|true|false|null)"
    _query = fr"(?:@\.\w+\s*==\s*{_vals})"

    _stmt_index = r"(?P<index>\d+)"
    _stmt_query = fr"(?P<query>\?\({_query}(?:\s*&&\s*{_query})*\))"

    RE_PAT = re.compile(
        fr"\.(?P<key>\w+)(?:\[(?:{_stmt_index}|{_stmt_query})\])*"
    )  # Super nasty regex pattern, so split into smaller patterns
    RE_IDX = RE_PAT.groupindex

    # Class methods
    @classmethod
    def parse_path(cls, path):
        """Parse paths, indices, and queries from a valid JSONPath.

        Parameters
        ----------
        path : str
            The valid JSONPath to parse out keys, indicies, and queries from.

        Returns
        -------
        dict{str:str}
            Returns a dictionary where the keys and values are the group names
            and the result, if any otherwise None, found from the regex
            operation.

        """
        matches = []
        for match in cls.RE_PAT.findall(path):
            match = [_ if _ != '' else None for _ in match]
            matches.append(dict(zip(cls.RE_IDX, match)))
        return matches

    @classmethod
    def insert_value(cls, path, value, record=None):
        """Insert a value at a specfied path into the given record.

        Parameters
        ----------
        path : str
            The path to insert the value at.
        value : any
            The value to insert.
        record : dict{str:any}
            The record to insert the value into.

        Returns
        -------
        dict{str:any}
            Returns the updated record.

        """
        record = {} if record is None else record

        def _get_index(key):
            matches = re.search(r"\[(?P<index>\d+)\]", key)
            if matches:
                return (
                    key.replace(matches.group(), ''),
                    int(matches.group('index')),
                )
            return None, None

        def _iter(keys=None, reference=None):
            keys = [] if keys is None else keys
            reference = {} if reference is None else reference

            if not keys:
                return

            key = keys.pop(0)
            index = _get_index(key)

            if index:
                key, idx = index
                if not key in reference:
                    reference[key] = []

                rlen = len(reference[key])
                if rlen <= idx:
                    for _ in range(idx + 1 - rlen):
                        reference[key].append({})

                ref = reference[key][idx]
                reference[key][idx] = _iter(keys, ref) if keys else value

            else:
                ref = reference.get(key, {})
                reference[key] = _iter(keys, ref) if keys else value

            return reference

        path_keys = path.split('.')
        if path_keys[0] == '$':
            path_keys.pop(0)

        record = _iter(path_keys, record)
        return record

    @classmethod
    def insert_query(cls, path, value, record=None):
        """Insert a value at a specfied path into the given record.

        This method is very similar to insert_value except it assumes the
        path includes a query. This method will then perform very similarly
        to insert_value, except it will ensure that the query is met when
        the value is inserted.

        Parameters
        ----------
        path : str
            The path to insert the value at.
        value : any
            The value to insert.
        record : dict{str:any}
            The record to insert the value into.

        Returns
        -------
        dict{str:any}
            Returns the updated record.

        """
        record = {} if record is None else record

        def _insert_value(*args): # pylint: disable=unused-argument
            return value

        path_keys = cls.parse_path(path)
        record = cls.iter_with_query(_insert_value, path_keys, record)

        return record

    @classmethod
    def iter_with_query(cls, func_to_execute, keys=None, reference=None):
        """Iterate through a given object based on a path of keys and executes a function at path.

        This method is very similar to the _iter method in the insert_value method except:
            - It assumes the keys includes a query.
            - It takes in a function reference to execute, instead of just inserting a value.
            - The function's return value will write to the specified path.

        Note, this is an iterative function and is called recursively.

        Parameters
        ----------
        func_to_execute : function ref
            The function to execute at the specified path. Return value will be written to path.
        keys : list(dict)
            The list of json keys, indexes, and queries to navigate and build out the record.
        reference : dict{str:any}
            The record (or sub-record) to insert the value into.

        Returns
        -------
        dict{str:any}
            Returns the updated record.

        """

        keys = [] if keys is None else keys
        reference = {} if reference is None else reference
        key, index, query = keys.pop(0).values()

        # convert index to integer, if exists
        if index is not None:
            index = int(index)

        # 4 possible cases:
        #    (a) query w/ index :     process query and update only that index from result
        #    (b) query w/o index: :   process query and update all values
        #    (c) just index :         grab just that index
        #    (d) only a key given :   treat like a dict key and update that value

        if query is not None:
            conditions = [
                tuple(
                    t.strip()
                        .replace('@.', '')
                        .replace('\'', '')
                        .replace('"', '')
                        .strip()
                    for t in s.strip().split('==')
                )
                for s in query[2:-1].split('&&')
            ]

            if not key in reference:
                reference[key] = []

            indices = []
            for i, ele in enumerate(reference[key]):
                if all(ele.get(k) == v for k, v in conditions):
                    indices.append(i)

            # Case A - Query w/ Index
            if index is not None:
                rlen = len(indices)
                if rlen <= index:
                    for _ in range(index + 1 - rlen):
                        reference[key].append(dict(conditions))
                    indices.append(-1)
                    index = -1

                ref = reference[key][indices[index]]
                if keys:
                    cls.iter_with_query(func_to_execute, list(keys), ref)
                else:
                    reference[key][indices[index]] = func_to_execute(reference[key][indices[index]])
            else:
                # Case B: Query w/out index
                if not indices:
                    reference[key].append(dict(conditions))
                    indices.append(-1)

                for idx in indices:
                    ref = reference[key][idx]
                    if keys:
                        cls.iter_with_query(func_to_execute, list(keys), ref)
                    else:
                        reference[key][index] = func_to_execute(reference[key][index])
        elif index is not None:
            # Case C: Index Only
            if not key in reference:
                reference[key] = []

            rlen = len(reference[key])
            if rlen <= index:
                for _ in range(index + 1 - rlen):
                    reference[key].append(
                        {}
                    )  # Change to type of child element

            ref = reference[key][index]
            if keys:
                cls.iter_with_query(func_to_execute, keys, ref)
            else:
                reference[key][index] = func_to_execute(reference[key][index])

        else:
            # Case D: Key Only
            ref = reference.get(key, {})
            if keys:
                cls.iter_with_query(func_to_execute, keys, ref)
            else:
                reference[key] = func_to_execute(reference.get(key, {}))
        return reference

    @classmethod
    def remove_duplicates(cls, path, record):
        """Remove duplicates within a record at the given path

        Parameters
        ----------
        path : str
            The path to deduplicate.
        record : dict{str:any}
            The record to update.

        Returns
        -------
        dict{str:any}
            Returns the updated record.

        """

        def _dedup_list(input_list: list):
            input_list = [] if input_list is None else input_list

            # For list of dictionaries
            if all(isinstance(d, dict) for d in input_list):
                return [dict(t) for t in {tuple(d.items()) for d in input_list}]
            # For list of value types
            return list(OrderedDict.fromkeys(input_list))

        path_keys = cls.parse_path(path)
        record = cls.iter_with_query(_dedup_list, path_keys, record)
        return record

    @classmethod
    def filter_record(cls, filters, record):
        """Applies the registered filters to the projected record.

        Parameters
        ----------
        filters : dict{FilterType:any}
            Dictionary defining the type of filter to apply and the path at which to apply
        record : dict{str:any}
            The object to apply filter to.

        Returns
        -------
        dict{str:any}
            Returns the updated record.

        """
        filters = {} if filters is None else filters
        record = {} if record is None else record

        for filter_type, paths in filters.items():
            if filter_type == FilterType.UNIQUE:
                for path in paths:
                    record = cls.remove_duplicates(path, record)

        return record

    # Instance attributes
    def __init__(self, manifest: JSONManifest):
        self._manifest = manifest

    # Instance methods
    def get_projection(self):
        """Generate the projection for the given manifest.

        Returns
        -------
        dict{str:any}
            Returns the generated projected json for the given manifest.

        """
        queries, record = [], {}
        for path, value in self._manifest:

            # Prioritize non-queries before queries
            if '?' in path:
                queries.append((path, value))
                continue

            self.insert_value(path, value, record)

        for path, value in queries:
            self.insert_query(path, value, record)

        # Handle Filters
        record = self.filter_record(self._manifest.filters, record)

        return record
