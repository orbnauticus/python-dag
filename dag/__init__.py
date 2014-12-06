#!/usr/bin/env python


class CycleError(Exception):
    def __init__(self, members):
        self.members = members

    def __str__(self):
        nodes = {source for source, _ in self.members}
        sinks = dict(sorted(self.members))
        cycle = []
        first = sorted(nodes)[0]
        cycle.append(first)
        node = sinks[first]
        while node != first:
            cycle.append(node)
            node = sinks[node]
        return ', '.join(repr(node) for node in cycle)


class TopologicalSorter(object):
    def __init__(self, edges, start, endpoints):
        if start:
            relevant, children = set(), set(start)
            while not children <= relevant:
                relevant.update(children)
                children = {sink for source, sink in edges
                            if source in relevant}
            edges = {edge for edge in edges if edge[0] in relevant}
        if endpoints:
            relevant, parents = set(), set(endpoints)
            while not parents <= relevant:
                relevant.update(parents)
                parents = {source for source, sink in edges
                           if sink in relevant}
            edges = {edge for edge in edges if edge[1] in relevant}
        self.graph = set(edges)

    def __iter__(self):
        nodes_with_no_incoming_edges = {
            source for source, _ in self.graph
            if source not in {s for _, s in self.graph}
        }
        while nodes_with_no_incoming_edges:
            yield nodes_with_no_incoming_edges
            outgoing_edges = {edge for edge in self.graph
                              if edge[0] in nodes_with_no_incoming_edges}
            self.graph -= outgoing_edges
            nodes_with_no_incoming_edges = {
                sink for _, sink in outgoing_edges
                if all(s != sink for _, s in self.graph)
            }
        if self.graph:
            raise CycleError(self.graph)

    def __reversed__(self):
        nodes_with_no_outgoing_edges = {
            sink for _, sink in self.graph
            if sink not in {s for s, _ in self.graph}
        }
        while nodes_with_no_outgoing_edges:
            yield nodes_with_no_outgoing_edges
            incoming_edges = {edge for edge in self.graph
                              if edge[1] in nodes_with_no_outgoing_edges}
            self.graph -= incoming_edges
            nodes_with_no_outgoing_edges = {
                source for source, _ in incoming_edges
                if all(s != source for s, _ in self.graph)
            }
        if self.graph:
            raise CycleError(self.graph)



class DAG(set):
    '''
    Directed Acyclic Graph

    >>> graph = DAG()
    >>> graph.add(('a', 'b'))  # a before b
    >>> graph.add(('b', 'd'))  # b before d
    >>> graph.add(('a', 'c'))  # a before c
    >>> graph.add(('c', 'd'))  # c before d

    'a' ---> 'b'
      \        \
       v        v
      'c' ---> 'd'

    Find the order for the whole graph by calling topsort after nodes are added

    >>> for nodes in graph.topsort():
    ...   print(sorted(nodes))
    ['a']
    ['b', 'c']
    ['d']

    Search for a subset of the graph by providing one or more starting nodes

    >>> for nodes in graph.topsort('c'):
    ...   print(nodes)
    {'c'}
    {'d'}

    Pass a list of endpoints to order nodes in paths that lead to them

    >>> for nodes in graph.topsort(endpoints=['b']):
    ...   print(nodes)
    {'a'}
    {'b'}

    >>> len(list(graph.topsort('c', endpoints=['c'])))
    0

    Cycles are automatically detected and reported

    >>> graph.add(('d', 'c'))  # d before c

    >>> result = iter(graph.topsort())

    >>> print(next(result))
    {'a'}

    >>> print(next(result))
    {'b'}

    >>> print(next(result))
    Traceback (most recent call last):
    ...
    topsort.CycleError: 'c', 'd'

    '''
    def topsort(self, *start, **kwargs):
        """
        Topologically sort the graph, possibly from a starting node.

        Yields sets of nodes such that all edges starting in one group end in
        a subsequent group.

        If arguments are present, the results are limited to paths which
        lead to those nodes.
        """
        endpoints = kwargs.pop('endpoints', ())
        if kwargs:
            raise TypeError("topsort got unexpected keyword argument "
                            "{!r}".format(kwargs.popitem()[0]))
        return TopologicalSorter(self, start, endpoints)

    def edges_from(self, *nodes):
        return {edge for edge in self if edge[0] in nodes}

    def edges_to(self, *nodes):
        return {edge for edge in self if edge[1] in nodes}


__all__ = ['DAG', 'CycleError']


if __name__=='__main__':
    import doctest
    doctest.testmod()
