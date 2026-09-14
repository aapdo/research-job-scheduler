"""Immutable wrapper code travels in the future job/attempt, not a mutable file."""
def wrap_argv(argv, code):
    argv=list(argv)
    if '-c' in argv:
        if code in argv:return argv
        raise ValueError('cannot wrap an existing Python command')
    i=argv.index('-B')
    if not argv[i+1].endswith('/worker.py'):
        raise ValueError('unsupported inline-validation entry')
    return argv[:i+1]+['-c',code]+argv[i+1:]
