
/* 
Copyright © 2017 Yurii Bilyk. All rights reserved. Contacts: <yuryk531@gmail.com>

This file is part of "Database integrator".

"Database integrator" is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

"Database integrator" is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with "Database integrator".  If not, see <http:www.gnu.org/licenses/>. 
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPARQLtoSQL
{
    public class SparqlGraph
    {
        public Node Root { get; set; }

        public List<Node> TopologicalSort()
        {
            //clean Nodes' topological marks
            List<Node> allNodes = GetAllNodes(null);
            foreach(var node in allNodes)
            {
                node.NodeMark = Node.TopologicalMark.UNMARKED;
            }

            List<Node> sorted = new List<Node>();

            while(allNodes.Count > 0) //while there're unmarked nodes
            {
                Node curr = allNodes[0];
                allNodes.RemoveAt(0);

                Visit(curr, sorted);
            }

            return sorted;
        }

        private void Visit(Node n, List<Node> sortedNodes)
        {
            if (n.NodeMark == Node.TopologicalMark.TMP)
                return;
            if(n.NodeMark == Node.TopologicalMark.UNMARKED)
            {
                n.NodeMark = Node.TopologicalMark.TMP;
                //as in this particular graph we have only ONE parent node, we have only one edge from some node 'n' to this one
                if (n.Parent != null)
                    Visit(n.Parent, sortedNodes);
                n.NodeMark = Node.TopologicalMark.PERMANENT;
                sortedNodes.Insert(0, n);
            }
        }

        public List<Node> GetAllNodes(Type nodeType)
        {
            List<Node> nodesList = new List<Node>();

            Stack<Node> nodes = new Stack<Node>();
            nodes.Push(Root);
            while(nodes.Count > 0)
            {
                Node curr = nodes.Pop();
 
                if (nodeType == null)
                {
                    nodesList.Add(curr);
                }
                else if (curr.GetType() == nodeType)
                {
                    nodesList.Add(curr);
                }

                if (curr._children != null)
                {
                    foreach (var node in curr._children)
                    {
                        nodes.Push(node);
                    }
                }
            }

            return nodesList;
        }
    }

    public abstract class Node
    {
        public enum TopologicalMark
        {
            UNMARKED,
            TMP,
            PERMANENT
        };

        public Node Parent { get; set; }
        public List<Node> _children = null; //!!!
        public List<Node> Children { get { return this._children ?? new List<Node>(); } }
        public TopologicalMark NodeMark { get; set; } = TopologicalMark.UNMARKED;
        public Node (Node parent)
        {
            this.Parent = parent;
            if(parent != null)
            {
                if(parent._children == null)
                {
                    parent._children = new List<Node>();
                }
                parent._children.Add(this);
            }
        }

        public override string ToString()
        {
            return this.GetType().Name.Replace("Node", "");
        }
    }

    public class TerminalNode : Node
    {
        public string Subj { get; set; }
        public string Pred { get; set; }
        public string Obj { get; set; }

        public TerminalNode(Node parent, string subj, string pred, string obj) : base(parent)
        {
            this.Subj = subj;
            this.Pred = pred;
            this.Obj = obj;
        }

        public override string ToString()
        {
            return $"{Subj} {Pred} {Obj}";
        }
    }

    public class JoinNode : Node
    {
        public List<string> Variables { get; set; } = new List<string>();

        public JoinNode(Node parent) : base(parent) { }

        public JoinNode(Node parent, Node child1, Node child2) : base(parent)
        {
            Children.Add(child1);
            Children.Add(child2);

            
        }
    }

    public class ProjectNode : Node
    {
        public readonly List<string> ProjectionVariables;

        public ProjectNode(Node parent, List<string> projectVariable) : base(parent)
        {
            this.ProjectionVariables = projectVariable;
        }
    }

    public class OptionalNode : Node
    {
        public string JoinCondition { get; set; }

        //Optional(n1,n2,e) => LeftJoin(n1,n2,e)
        public OptionalNode(Node parent, Node child, string joinCondition) : base(parent)
        {
            Children.Add(child);
            JoinCondition = joinCondition;
        }
    }

    public class UnionNode : Node
    {
        public UnionNode(Node parent) : base(parent) { }
    }

    public class FilterNode : Node
    {
        public string FilterExpression { get; set; }
        public FilterNode(Node parent) : base(parent) { }
    }
}
