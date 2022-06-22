import numpy as np

def read_txt(text):
    with open(text) as f:
        text=[word for line in f for word in line.split()]
    return text


def palindrome(section):
    count = 0
    max_len = 0
    for word in section:
        if word == word[::-1]:
            count+=1
            temp = len(word)
            if temp > max_len:
                max_len = temp
    return count, max_len

def txt_rows(text):
    with open(text) as f:
         lines = [line[:-1] for line in f]
    return lines

def sort_txt(section):
    sorted_txt = sorted(section, key=None)
    with open("test_sorted.txt", "w") as txt_file:
        for line in sorted_txt:
            txt_file.write(line + "\n")



if __name__ == "__main__":
    text = read_txt("test.txt")
    n_palindromes, max_len = palindrome(text)
    text_rows = txt_rows("test.txt")
    sort_txt(text_rows)