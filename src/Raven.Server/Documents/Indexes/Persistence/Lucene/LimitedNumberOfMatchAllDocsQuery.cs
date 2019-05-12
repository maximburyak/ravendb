using System;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Query = Lucene.Net.Search.Query;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public class LimitedNumberOfMatchAllDocsQuery : Query
    {
        public int MinPageSize;

        private class LimitedNumberOfMatchAllScorer : Scorer
        {
            private int _numberOfResults;

            private void InitBlock(LimitedNumberOfMatchAllDocsQuery enclosingInstance)

            {

                this.enclosingInstance = enclosingInstance;

            }

            private LimitedNumberOfMatchAllDocsQuery enclosingInstance;

            public LimitedNumberOfMatchAllDocsQuery Enclosing_Instance
            {
                get
                {
                    return enclosingInstance;
                }
            }

            internal TermDocs termDocs;

            internal float score;

            private int doc = -1;

            internal LimitedNumberOfMatchAllScorer(LimitedNumberOfMatchAllDocsQuery enclosingInstance, IndexReader reader, Similarity similarity, Weight w, IState state) : base(similarity)
            {

                InitBlock(enclosingInstance);

                this.termDocs = reader.TermDocs(null, state);

                score = w.Value;
            }

            public override int DocID()
            {
                return doc;
            }

            public override int NextDoc(IState state)
            {
                if (_numberOfResults++ > Enclosing_Instance.MinPageSize)
                    return NO_MORE_DOCS;

                return doc = termDocs.Next(state) ? termDocs.Doc : NO_MORE_DOCS;
            }

            public override float Score(IState state)
            {
                return score;
            }

            public override int Advance(int target, IState state)
            {
                return doc = termDocs.SkipTo(target, state) ? termDocs.Doc : NO_MORE_DOCS;                
            }

        }


        private class LimitedNumberOfMatchAllDocsWeight : Weight
        {
            private void InitBlock(LimitedNumberOfMatchAllDocsQuery enclosingInstance)
            {
                this.EnclosingInstance = enclosingInstance;
            }            

            public LimitedNumberOfMatchAllDocsQuery EnclosingInstance { get; private set; }

            private Similarity similarity;

            private float queryWeight;

            private float queryNorm;

            public LimitedNumberOfMatchAllDocsWeight(LimitedNumberOfMatchAllDocsQuery enclosingInstance, Searcher searcher)
            {
                InitBlock(enclosingInstance);

                this.similarity = searcher.Similarity;
            }

            public override System.String ToString()
            {
                return "weight(" + EnclosingInstance + ")";
            }

            public override Query Query
            {
                get { return EnclosingInstance; }
            }

            public override float Value => queryWeight;

            public override float GetSumOfSquaredWeights()
            {
                queryWeight = EnclosingInstance.Boost;
                return queryWeight * queryWeight;
            }

            public override void Normalize(float queryNorm)
            {
                this.queryNorm = queryNorm;
                queryWeight *= this.queryNorm;
            }

            public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
            {
                return new LimitedNumberOfMatchAllScorer(EnclosingInstance, reader, similarity, this, state);
            }

            public override Explanation Explain(IndexReader reader, int doc, IState state)
            {
                // explain query weight

                Explanation queryExpl = new ComplexExplanation(true, Value, "MatchAllDocsQuery, product of:");

                if (EnclosingInstance.Boost != 1.0f)
                {
                    queryExpl.AddDetail(new Explanation(EnclosingInstance.Boost, "boost"));
                }

                queryExpl.AddDetail(new Explanation(queryNorm, "queryNorm"));

                return queryExpl;
            }
        }

        public override Weight CreateWeight(Searcher searcher, IState state)
        {
            return new LimitedNumberOfMatchAllDocsWeight(this, searcher);
        }

        public override void ExtractTerms(System.Collections.Generic.ISet<Term> terms)
        {
        }

        public override System.String ToString(System.String field)
        {
            System.Text.StringBuilder buffer = new System.Text.StringBuilder();
            buffer.Append("*:*");
            buffer.Append(ToStringUtils.Boost(Boost));
            return buffer.ToString();
        }

        public override bool Equals(System.Object o)
        {
            if (!(o is MatchAllDocsQuery))

                return false;

            MatchAllDocsQuery other = (MatchAllDocsQuery)o;

            return this.Boost == other.Boost;
        }

        public override int GetHashCode()
        {
            return BitConverter.ToInt32(BitConverter.GetBytes(Boost), 0) ^ 0x1AA71190;
        }

    }
}
