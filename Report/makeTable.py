"""
This module allows one to easily make a table in both HTML and plain-text
"""

import re
import types

ftoa_re = re.compile(r'(?<=\d)(?=(\d\d\d)+(\.|$))')
def ftoa(s):
    """
    Converts a float or integer to a properly comma-separated string using
    regular expressions.  Kudos to anyone who can actually read the RE.
    """
    return ftoa_re.sub(',', str(s))

class Table:

    def __init__(self, add_numbers = True):
        self.headers = []
        self.headerLengths = []
        self.maxHeaderLen = 0
        self.data = []
        self.colors = []
        self.headerLines = 1
        self.types = []
        self.rowCtr = 0
        self.add_numbers = add_numbers

    def setHeaders(self, headers):
        if self.add_numbers:
            headers.insert(0, " ")
        self.headerLengths = []
        for header in headers:
            splits = header.splitlines()
            try:
                header_len = max([len(i) for i in splits])
            except:
                header_len = 1
            self.headerLines = max(self.headerLines, len(splits))
            self.headerLengths.append(header_len)
            self.headers.append(splits)
        self.maxHeaderLen = max(self.headerLengths)

    def formatEntry(self, entry):
        if isinstance(entry, types.TupleType):
            val = entry[0]
        else:
            val = entry
        if isinstance(val, types.IntType) or \
                isinstance(val, types.LongType) or \
                isinstance(val, types.FloatType):
            val = ftoa(val)
        if isinstance(entry, types.TupleType):
            return (str(val), ) + entry[1:]
        else:
            val = str(val)
            return val

    def entryType(self, entry):
        if isinstance(entry, types.IntType) or \
                isinstance(entry, types.LongType):
            return int
        elif isinstance(entry, types.FloatType):
            return float
        return str

    def addRow(self, data, colors=None):
        self.rowCtr += 1
        if self.add_numbers:
            data.insert(0, self.rowCtr)
        assert len(data) == len(self.headerLengths)
        if colors:
            if self.add_numbers:
                colors.insert(0, None)
            assert len(data) == len(colors)
        mytypes = [self.entryType(i) for i in data]
        self.types.append(mytypes)
        data = [self.formatEntry(i) for i in data]
        for i in range(len(data)):
            if isinstance(data[i], types.TupleType):
                mylen = len(data[i][0])
            else:
                mylen = len(data[i])
            self.headerLengths[i] = max(self.headerLengths[i], mylen)
        self.data.append(data)
        self.colors.append(colors)

    def addBreak(self):
        self.data.append(None)

    def plainTextHeader(self):
        table_len = 1 + sum([i+3 for i in self.headerLengths])
        output = '-' * table_len + '\n'
        for i in range(self.headerLines):
            output += '|'
            ctr = 0
            for header in self.headers:
                if len(header) <= i:
                    header = ''
                else:
                    header = header[i]
                output += ' %s |' % header.center(self.headerLengths[ctr])
                ctr += 1
            output += '\n'
        output += '-' * table_len + '\n'
        return output
     
    def plainTextFooter(self):
        table_len = 1 + sum([i+3 for i in self.headerLengths])
        output = '-' * table_len + '\n'
        return output

    perc_re = re.compile(r"-?(\d+)%")
    def plainTextBody(self):
        header_cnt = len(self.headers)
        output = ''
        idx = 0
        table_len = sum([i+3 for i in self.headerLengths])-1
        for row in self.data:
            rowtypes = self.types[idx]
            output += '|'
            if row == None:
                output += '-' * table_len + '|\n'
                continue
            for i in range(header_cnt):
                if isinstance(row[i], types.TupleType):
                    val = row[i][0]
                else:
                    val = row[i]
                if rowtypes[i] == types.StringType and not \
                        self.perc_re.match(val):
                    output += (' %%-%is |' % self.headerLengths[i]) % \
                        val
                else:
                    output += (' %%%is |' % self.headerLengths[i]) % \
                        val
            output += '\n'
            idx += 1
        return output

    def plainText(self):
        return self.plainTextHeader() + self.plainTextBody() + \
            self.plainTextFooter()

    def html(self, css_class="mytable"):
        header = ''
        for entry in self.headers:
            header += "<th>%s</th>" % ' '.join([i.replace('\n','<br/>') for i \
                in entry])
        output = """<table class="%s">\n\t<thead>%s</thead>\n""" % \
            (css_class, header)
        ctr = 0
        add_thick_border = False
        for row in self.data:
            if row == None:
                add_thick_border = True
                continue
            if add_thick_border:
                output += '\t<tr style="border-top-width: %spx"> ' % '3'
            else:
                output += "\t<tr> "
            col_ctr = 0
            rowtypes = self.types[col_ctr]
            for entry in row:
                align = 'right'
                color = 'white'
                if rowtypes[col_ctr] == types.StringType:
                    align = 'left'
                if self.colors[ctr] and self.colors[ctr][col_ctr]:
                    color = self.colors[ctr][col_ctr]
                if isinstance(entry, types.TupleType):
                    entry = '<a href="%s">%s</a>' % (entry[1], entry[0])
                output += '<td style="background-color: %s; text-align: %s;'\
                    ' border-top-width: %spx;">%s</td>' % (color, align,
                    int(add_thick_border)*3, entry)
                col_ctr += 1
            output += " </tr>\n"
            if add_thick_border == True:
                add_thick_border = False
            ctr += 1
        output += "</table>"
        return output