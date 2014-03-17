def quote_string(word, quote_char='"'):
    return '%s%s%s' % (quote_char, word.replace('%s' % quote_char,  r'\%s' % quote_char), quote_char)

def smart_quote_string(word):
    test_char = word[0]
    if test_char in ("'", '"') and test_char == word[-1] and word[-2] != '\\':
        return word
    else:
        return quote_string(word)
