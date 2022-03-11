g = [
    [0, 1, 0, 0, 0, 0, 0, 0],
    [0, 0, 1, 0, 0, 0, 0, 0],
    [1, 0, 0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 1, 0, 0, 1],
    [0, 0, 0, 0, 0, 1, 0, 0],
    [1, 0, 0, 0, 0, 0, 1, 0],
    [1, 0, 1, 0, 1, 0, 0, 0],
    [0, 0, 0, 1, 0, 1, 0, 0]]


def tarjan_dfs(i, dfn, low, stack, inStack, ts):
    dfn[i] = low[i] = ts
    ts += 1
    stack.append(i)
    inStack[i] = True

    for y in range(0, len(g[i])):
        if g[i][y]:
            if dfn[y] == 0:
                tarjan_dfs(y, dfn, low, stack, inStack, ts)
                low[i] = min(low[i], low[y])
            elif inStack[y]:
                low[i] = min(low[i], dfn[y])

    print(stack)
    print(dfn)
    print(low)
    if dfn[i] == low[i]:
        tmp = -1
        while tmp != i:
            tmp = stack.pop()
            inStack[tmp] = False
            print(tmp, "-")
        print("")


if __name__ == '__main__':
    dfn = [0] * 8
    low = [0] * 8
    visited = [False] * 8
    inStack = [False] * 8
    stack = []
    ts = 1
    for i in range(0, 8):
        if not dfn[i]:
            tarjan_dfs(i, dfn, low, stack, inStack, ts)
