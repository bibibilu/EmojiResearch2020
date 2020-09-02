import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import collections
import numpy as np

def read_in_network(input_path):
    """
    [5  points] In this function, which could be very short, you should read in  the facebook text file as a
    networkX graph object. See what I said above about the graph being undirected. Hint: there is a very nice
    and elegant way in networkX to read edge lists directly from file into a graph object.
    :param input_path: the path to the decompressed facebook_combined txt file.
    :return: a networkX graph object G
    """
    G = nx.Graph()

    # load file
    f =  open(input_path, 'r')
    edges = f.readlines()
    f.close()

    # add all edges
    for row in edges:
        string_list = row.rstrip('\n').split(",")  # ['0', '1']
        G.add_edge(string_list[0],string_list[1])  #  Each line is an edge e.g., 0 10 means that node 0 is linked to node 10.

    return G


def degree_distribution(G, filename, extrainfo = False):
    """
    [15 points] Plot the  degree distribution on a log-log plot. You can use python (e.g., matplotlib and pyplot if you
    wish for the plot, or collect data  and plot it somewhere else, like excel).
    Does the degree distribution obey the power
    law well? If  so, what is the power-law coefficient? (Recall that a power law says that the prob(k) of degree
    k is proportional to k^{-gamma}. Gamma is known as  the power-law coefficient (it also has other names).

    The plot should be separately included with  your homework submission (a short PDF report, containing answers/figures,
    with captions to any questions that cannot be directly answered in file here)
    :param G: the networkX graph object
    :return: None
    """
    degree_sequence = [d for n, d in G.degree()]
    degreeCount = collections.Counter(degree_sequence)
    deg, cnt = zip(*degreeCount.items())

    if extrainfo:
        max_deg = max(deg) #1045
        max_node = degree_sequence.index(max_deg)  #107
        print("The highest degree node have:", max_deg, "friends")
        print("The clustering coefficient for max degree node is: ", nx.clustering(G, nodes= max_node))

    xlog = np.log(deg)
    ylog = np.log(cnt)

    # Fit linear curve, find R2
    model = LinearRegression().fit(xlog.reshape(-1, 1),ylog.reshape(-1, 1))
    b0 = model.intercept_
    b1 = model.coef_
    print("Coefficient ", b0[0], b1[0][0])
    print("Linear fit: R2: ", model.score(xlog.reshape(-1, 1), ylog.reshape(-1, 1)))
    x = np.linspace(0, 8, 100)
    y = b0 + b1*x
    label_sting = "f(x) =" +str(round(b0[0],3))+"+"+str(round(b1[0][0],3))+"*x"
    plt.plot(x,y.reshape(-1, 1), label=label_sting)
    plt.legend(loc='best')

    plt.scatter(xlog, ylog, c= 'red', s = 1.2)
    plt.title("Degree Distribution - Log log")
    plt.ylabel("Log(Count)")
    plt.xlabel("Log(Degree)")
    plt.grid(True)
    plt.savefig(filename)
    plt.close()
    return None

def evaluate_G(G):
    """
    [25 points] In this function, write code (you can use networkX functionality to any extent that you wish) to verify
    the numbers on the SNAP webpage describing this dataset regarding the average clustering coefficient, the number of
    triangles, the diameter, and the nodes and edges in the largest connected component. Is there any discrepancy?
    Please report if so.
    :param G: the networkX graph object
    :return: None
        Nodes	4039
        Edges	88234
        Nodes in largest WCC	4039 (1.000)
        Edges in largest WCC	88234 (1.000)
        Nodes in largest SCC	4039 (1.000)
        Edges in largest SCC	88234 (1.000)
        Average clustering coefficient	0.6055
        Number of triangles	1612010
        Fraction of closed triangles	0.2647
        Diameter (longest shortest path)	8
        90-percentile effective diameter	4.7
    """
    print("Nodes", G.number_of_nodes())
    print("Edges", G.number_of_edges())
    print("Graph is undirect. Connected components is : ", nx.is_connected(G))

    if nx.is_connected(G):
        print("Nodes in largest SCC:", G.number_of_nodes())
        print("Edges in largest SCC:", G.number_of_edges())
        print("Diameter (longest shortest path)", nx.diameter(G))

    print("Average clustering coefficient", nx.average_clustering(G))

    #num_traingle = int(sum(nx.triangles(G).values())/3)
    #print("Number of triangles", num_traingle)

    #ratio_triangle = nx.transitivity(G) / 3
    #frac_triangles = 1/((1/ratio_triangle) -2) # remove duplicate count of triangles
    #print("Fraction of closed triangles ", frac_triangles)  # https://networkx.github.io/documentation/stable/reference/algorithms/generated/networkx.algorithms.cluster.transitivity.html?highlight=triangles

    return None

# ALL EMOJIS
# input_path = "./emoji_concurrence_0306.txt/part-00000"  # 306527
# G_all  = read_in_network(input_path)
#
# degree_distribution(G_all, "degree_dist_all", extrainfo = False)
#
# evaluate_G(G_all)
'''
Singletons = 1436
Nodes 1757
Edges 62554
Graph is undirect. Connected components is :  False
Coefficient  5.424924247897512 -0.9045795450996645
Linear fit: R2:  0.8295159139775949
Average clustering coefficient 0.5375845685742442
fraction of singleton = 1436/ (1436+1757) = 0.4497
'''

# [len(c) for c in sorted(nx.connected_components(G_all), key=len, reverse=True)]  # [1746, 3, 2, 2, 2, 2]
# largest_cc = max(nx.connected_components(G_all), key=len)

# TOP 5 Lans

# ENGLISH
# G_en  = read_in_network('/Users/claire/Desktop/599/project/research/network/emoji_concurrence_en_0305/part-00000')
# degree_distribution(G_en, "degree_dist_en", extrainfo = False)
# evaluate_G(G_en)
'''
Singletons = 1015
Nodes 1380
Edges 25462
Graph is undirect. Connected components is :  False
Coefficient  5.602655820278304 -1.021045286167557
Linear fit: R2:  0.837091095518916
Average clustering coefficient 0.5476018457965102
fraction of singleton = 1015/ (1015+1380) = 0.4238
'''

# [len(c) for c in sorted(nx.connected_components(G_en), key=len, reverse=True)]  # [1372, 6, 2]
# largest_cc = max(nx.connected_components(G_en), key=len)


# Japanese
# G_ja = read_in_network('/Users/claire/Desktop/599/project/research/network/emoji_concurrence_ja_0305/part-00000')
# degree_distribution(G_ja, "degree_dist_ja", extrainfo=False)
# evaluate_G(G_ja)
'''
Singletons = 745
Nodes 1058
Edges 20618
Graph is undirect. Connected components is :  False
Coefficient  5.024851946063894 -0.9208751519784002
Linear fit: R2:  0.8309265997999904
Average clustering coefficient 0.5899574008304421
fraction of singleton = 745/ (745+1058) = 0.4132
'''

# [len(c) for c in sorted(nx.connected_components(G_ja), key=len, reverse=True)]  # [1050, 2, 2, 2, 2]
# largest_cc = max(nx.connected_components(G), key=len)


# Spanish
# G_es  = read_in_network('/Users/claire/Desktop/599/project/research/network/emoji_concurrence_es_0305/part-00000')
# degree_distribution(G_es, "degree_dist_es", extrainfo = False)
# evaluate_G(G_es)
'''
Singletons = 708
Nodes 977
Edges 12791
Graph is undirect. Connected components is :  False
Coefficient  5.270159696769594 -1.0254586697549097
Linear fit: R2:  0.8611331955015536
Average clustering coefficient 0.519254058733096
fraction of singleton = 708/ (708+977) = 0.4202
'''

# [len(c) for c in sorted(nx.connected_components(G_es), key=len, reverse=True)]  # [965, 2, 2, 2, 2, 2, 2]
# largest_cc = max(nx.connected_components(G_es), key=len)


# Korean
# G_ko  = read_in_network('/Users/claire/Desktop/599/project/research/network/emoji_concurrence_ko_0305/part-00000')
# degree_distribution(G_ko, "degree_dist_ko", extrainfo = False)
# evaluate_G(G_ko)
'''
Singletons = 324
Nodes 708
Edges 5103
Graph is undirect. Connected components is :  False
Coefficient  5.489800497320582 -1.2026885301591794
Linear fit: R2:  0.8608739751210907
Average clustering coefficient 0.49862954509453633
fraction of singleton = 324/ (324+708) = 0.3140
'''

# [len(c) for c in sorted(nx.connected_components(G_ko), key=len, reverse=True)]  # [698, 2, 2, 2, 2, 2]
# largest_cc = max(nx.connected_components(G_ko), key=len)


# German
# G_und  = read_in_network('/Users/claire/Desktop/599/project/research/network/emoji_concurrence_und_0305/part-00000')
# degree_distribution(G_und, "degree_dist_und", extrainfo = False)
# evaluate_G(G_und)
'''
Singletons = 761
Nodes 1023
Edges 10289
Graph is undirect. Connected components is :  False
Coefficient  5.714179981982361 -1.1413777966500338
Linear fit: R2:  0.8457790745253171
Average clustering coefficient 0.5313880954607948
fraction of singleton = 761/ (761+1023) = 0.4266
'''

# [len(c) for c in sorted(nx.connected_components(G_und), key=len, reverse=True)]  # [1000, 4, 3, 3, 3, 2, 2, 2, 2, 2]
# largest_cc = max(nx.connected_components(G_und), key=len)


# Arabic
# G_ar  = read_in_network('/Users/claire/Desktop/599/project/research/network/emoji_concurrence_ar_0305/part-00000')
# degree_distribution(G_ar, "degree_dist_ar", extrainfo = False)
# evaluate_G(G_ar)
'''
Singletons = 564
Nodes 927
Edges 7668
Graph is undirect. Connected components is :  False
Coefficient  5.603527386129631 -1.1724166141455379
Linear fit: R2:  0.8583166137732945
Average clustering coefficient 0.5018502479498576
fraction of singleton = 564/ (564+927) = 0.3783
'''

# [len(c) for c in sorted(nx.connected_components(G_ar), key=len, reverse=True)]  # [912, 3, 2, 2, 2, 2, 2, 2]
# largest_cc = max(nx.connected_components(G_ar), key=len)


# Portuguese
# G_pt  = read_in_network('/Users/claire/Desktop/599/project/research/network/emoji_concurrence_pt_0305/part-00000')
# degree_distribution(G_pt, "degree_dist_pt", extrainfo = False)
# evaluate_G(G_pt)
'''
Singletons = 540
Nodes 702
Edges 5868
Graph is undirect. Connected components is :  False
Coefficient  4.864042392725556 -1.0437521195167851
Linear fit: R2:  0.7772863298009046
Average clustering coefficient 0.530352061454253
fraction of singleton = 540/ (540+702) = 0.4348
'''

# [len(c) for c in sorted(nx.connected_components(G_pt), key=len, reverse=True)]  # [671, 7, 4, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
# largest_cc = max(nx.connected_components(G_pt), key=len)