
groupped: dict[str, list[str]] = {}

with open('/tmp/thread_context.txt') as f:
    for line in f:
        parts = line.strip().split(' ')
        id = parts[-1]
        action = ' '.join(parts[0:-1])
        actions = groupped.get(id, [])
        actions.append(action)
        groupped[id] = actions

for _, actions in groupped.items():
    for i, a in enumerate(actions):
        if a == '~thread_context':
            if i != len(actions) - 1:
                print("YOYOOYOOY")
                print(actions)
            

print("done")

