import base.Util as util

def log(content):
    string = "[Time: " + util.getYMDHMS() + "]: " + str(content)
    print(string)
    file = open("log.out", "a")
    file.write(string + "\n")
    file.close()
    return string