import re
import sys

CONFLICT = re.compile(
    r'<<<<<<< HEAD\n(.*?)\n=======\n(.*?)\n>>>>>>> [0-9a-f]+\n',
    re.DOTALL,
)


def resolve_changelog(path):
    s = open(path, encoding='utf-8').read()
    m = CONFLICT.search(s)
    head, theirs = m.group(1), m.group(2)
    # head = new-feature bullet(s) already on master + full version history.
    # Split at the first released version header so the incoming bullet stays
    # under [Unreleased], above the released sections.
    idx = head.index('## [3.6.1]')
    unreleased_bullets = head[:idx].rstrip()
    history = head[idx:]
    incoming = theirs.strip()
    new_region = unreleased_bullets + '\n' + incoming + '\n\n' + history
    s = s[:m.start()] + new_region + s[m.end():]
    open(path, 'w', encoding='utf-8').write(s)


def resolve_append(path):
    # Both sides added independent doc sections at the same spot; keep both.
    s = open(path, encoding='utf-8').read()
    while True:
        m = CONFLICT.search(s)
        if not m:
            break
        merged = m.group(1).rstrip() + '\n\n' + m.group(2).strip() + '\n'
        s = s[:m.start()] + merged + s[m.end():]
    open(path, 'w', encoding='utf-8').write(s)


if __name__ == '__main__':
    kind, path = sys.argv[1], sys.argv[2]
    if kind == 'changelog':
        resolve_changelog(path)
    else:
        resolve_append(path)
    remaining = sum(1 for l in open(path, encoding='utf-8') if l.startswith(('<<<<<<<', '=======', '>>>>>>>')))
    print(f'{path}: {remaining} markers remaining')
