import sys
import re
import subprocess
import tempfile


class TagsMasks(object):
    @classmethod
    def _parse(cls, input):
        with_min_value = []
        without_min_value = []

        for lineno, line in enumerate(input, 1):
            regexp_str = None
            min_value = None
            fields = line.rstrip('\n').split('\t')

            if len(fields) == 1:
                regexp_str = fields[0]
            elif len(fields) == 2:
                regexp_str = fields[0]
                try:
                    min_value = int(fields[1])
                except ValueError as e:
                    raise ValueError("Bad min_value '%s' on line %d: %s" % (fields[1], lineno, e))

                if min_value < 0:
                    raise ValueError("min_value is negative on line %d" % lineno)
            else:
                raise ValueError("Malformed line %d" % lineno)

            try:
                regexp = re.compile(regexp_str)
            except Exception as e:
                raise ValueError("Malformed regexp '%s' on line %d: %s" % (regexp_str, lineno, e))

            if min_value is None:
                if regexp.groups:
                    raise ValueError("No min_value but regexp contains groups '%s' on line %d" % (regexp_str, lineno))
            elif regexp.groups != 1:
                raise ValueError("min_value without groups in regexp '%s' on line %d" % (regexp_str, lineno))

            if min_value is None:
                without_min_value.append(regexp)
            else:
                with_min_value.append((regexp, min_value))

        return (with_min_value, without_min_value)

    @classmethod
    def parse(cls, input):
        with_min_value, without_min_value = cls._parse(input)

        if not with_min_value and not without_min_value:
            return cls.get_empty_matcher()

        pattern = re.compile(
            '^(?:' \
            + '|'.join([r.pattern for r in [r for r, _ in with_min_value] + without_min_value]) \
            + ')$'
        )

        min_values = [v for _, v in with_min_value]

        def match(str):
            m = pattern.match(str)
            if not m:
                #print >>sys.stderr, '+ not matched at all'
                return False

            if m.lastindex is None:
                #print >>sys.stderr, '+ lastindex is None (without_min_value)'
                return True

            group_idx = m.lastindex - 1

            matched_group = m.groups()[group_idx]
            #print >>sys.stderr, '+ matched_group = %s' % matched_group
            try:
                val = int(matched_group)
            except ValueError as e:
                raise RuntimeError("Initial regexp match non integer '%s' in '%s'" % (matched_group, str))

            #print >>sys.stderr, '+ val = %d, min_value = %d' % (val, min_values[group_idx])
            return val >= min_values[group_idx]

        match.count = len(with_min_value) + len(without_min_value)

        match.regexps = [r.pattern for r in without_min_value] \
            + [(r.pattern, min_value) for r, min_value in with_min_value]

        return match

    @classmethod
    def load(cls, location):
        with tempfile.NamedTemporaryFile(prefix='cloud_tags_list') as file:
            subprocess.check_call(
                ["svn", "export", "--force", "-q", "--non-interactive", location, file.name])

            with open(file.name) as input:
                return cls.parse(input)

    @staticmethod
    def get_empty_matcher():
        ret = lambda name: False
        ret.count = 0
        ret.regexps = []
        return ret

if __name__ == '__main__':
    from pprint import pprint
    pprint( parse(sys.stdin) )
