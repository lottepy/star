class CONSOLE_COLOR:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def PANIC(msg: str, exitcode=1):
    print(f"{CONSOLE_COLOR.FAIL}[ERROR] {msg}{CONSOLE_COLOR.ENDC}")
    # exit(exitcode)
    raise Exception("666")

def WARN(msg: str):
    print(f"{CONSOLE_COLOR.WARNING}[WARN] {msg}{CONSOLE_COLOR.ENDC}")

def INFO(msg: str):
    print(f"{CONSOLE_COLOR.OKBLUE}[INFO] {msg}{CONSOLE_COLOR.ENDC}")

def CLASS_STRING(o):
    attributes = ["=".join([attr, str(getattr(o, attr))]) for attr in dir(o) if not attr.startswith('__')]
    return ", ".join(attributes)