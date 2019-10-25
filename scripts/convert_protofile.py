# Copyright 2018-present RethinkDB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy of the
# License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
#
# This file incorporates work covered by the following copyright:
#
#     Copyright 2010-present, The Linux Foundation, portions copyright Google and
#     others and used with permission or subject to their respective license
#     agreements.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
'''Converts the protobuf file into proto_basic.json'''

import codecs
import json
import re
from collections import OrderedDict


def convert_protofile(proto_filename, proto_json_filename):
    with open(proto_filename) as ql2:
        proto = Proto2Dict(ql2)()
    with codecs.open(proto_json_filename, "w") as pb:
        json.dump(proto, pb, separators=(",", ": "), indent=2)


# Used in parsing protofile
MESSAGE_REGEX = re.compile('\s*(message|enum) (?P<name>\w+) \{')
VALUE_REGEX = re.compile('\s*(?P<name>\w+)\s*=\s*(?P<value>\w+)')
END_REGEX = re.compile('\s*\}')


class Proto2Dict(object):
    def __init__(self, input_file):
        self._in = input_file
        self.d = OrderedDict()
        self.parents = []

    def __call__(self):
        for line in self._in:
            (self.match_message(line) or
             self.match_value(line) or
             self.match_end(line))
        while self.parents:
            self.pop_stack()
        return self.d

    def push_message(self, name):
        new_level = OrderedDict()
        self.d[name] = new_level
        self.parents.append(self.d)
        self.d = new_level

    def pop_stack(self):
        self.d = self.parents.pop()

    def match_message(self, line):
        match = MESSAGE_REGEX.match(line)
        if match is None:
            return False
        self.push_message(match.group('name'))
        return True

    def match_value(self, line):
        match = VALUE_REGEX.match(line)
        if match is None:
            return False
        self.d[match.group('name')] = int(match.group('value'), 0)
        return True

    def match_end(self, line):
        if END_REGEX.match(line):
            self.pop_stack()
            return True
        else:
            return False


if __name__ == '__main__':
    import sys
    convert_protofile(sys.argv[1], sys.argv[2])
