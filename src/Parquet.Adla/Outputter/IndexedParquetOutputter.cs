using Microsoft.Analytics.Interfaces;

namespace Parquet.Adla.Outputter
{
   /// <summary>
   /// This outputter will change rowgroup not on an arbitary size but whenever the first column changes value
   /// it is imperative that the data is sorted by at least the first column to minimise the number of rowsets.
   /// 
   /// An additional rowset will be appended that containst just the first column value one row per original rowset
   /// this can be used as an index to determine the rowset of interest when looking up data by the first column value
   /// </summary>
   [SqlUserDefinedOutputter(AtomicFileProcessing = true)]
   public class IndexedParquetOutputter : IOutputter
   {
      private readonly IndexedDataSetBuilder _builder = new IndexedDataSetBuilder();

      public override void Output(IRow input, IUnstructuredWriter output)
      {
         _builder.Add(input, output.BaseStream);
      }

      public override void Close()
      {
         _builder.Dispose();
      }
   }
}