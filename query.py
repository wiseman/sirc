import re


# Parse single term queries and phrase queries ("word another word").


def s_ident(scanner, token): return token
def s_operator(scanner, token): return "op%s" % token
def s_float(scanner, token): return float(token)
def s_int(scanner, token): return int(token)

scanner = re.Scanner([
  (r"[a-zA-Z_]\w*", s_ident),
  (r"\d+\.\d*", s_float),
  (r"\d+", s_int),
  (r"=|\+|-|\*|/", s_operator),
  (r"\s+", None),
  ])

# sanity check
#print scanner.scan("sum = 3*foo + 312.50 + bar")



def s_phrase_query(scanner, token): return ['PHRASE', token[1:-1]]
def s_term_query(scanner, token): return ['TERM', token]

scanner = re.Scanner([
  (r'"(\s|[^_\W"])+"', s_phrase_query),
  (r'[^_\W"]+', s_term_query),
  (r'[\s_]+', None),
  ])

print scanner.scan('foo')
print scanner.scan('foo bar')
print scanner.scan('foo    bar')
print scanner.scan('foo_bar')
print scanner.scan('"foo bar"')

