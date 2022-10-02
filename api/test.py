from filelock import FileLock
import fileinput


def _replace_in_file(file_path, search_text, new_line):
    found = False
    # with FileLock(file_path):
    with fileinput.input(file_path, inplace=True) as file:
    # with open(file_path) as f:
    #    lines = f.readlines()
    #     print(lines)
        for line in file:
            if search_text in line:
                found = True
                print(new_line, end='')
            else:
                print(line, end='')
        # for ind, line in enumerate(lines):
        #     if search_text in line:
        #         found = True
        #         lines[ind] = new_line
    if not found:
        with open(file_path, 'a') as file:
            file.write(new_line + '\n')
    # if not found:
    #     lines.append(new_line + '\n')
    # with open(file_path, 'w') as file:
    #     for line in lines:
    #         file.write(line)


_replace_in_file('db.cashcash.app', 'test', '*.new  IN  A   10.0.0.2')
