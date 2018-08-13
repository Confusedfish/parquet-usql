using Microsoft.Analytics.Interfaces;
using Parquet.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Parquet.Adla.Outputter
{
   /// <summary>
   /// This data set builder will change rowgroup not on an arbitary size but whenever the first column changes value
   /// it is imperative that the data is sorted by at least the first column to minimise the number of rowsets.
   /// 
   /// An additional rowset will be appended that containst just the first column value one row per original rowset
   /// this can be used as an index to determine the rowset of interest when looking up data by the first column value
   /// </summary>
   internal sealed class IndexedDataSetBuilder : IDisposable
   {
      private Schema _schema;
      private ArrayList[] _ds;
      private ArrayList[] _keys;
      private ParquetWriter _writer;
      private int _fieldCount;
      private int _rowGroupCount = 0;

      private IComparable _lastKey = null;

      public void Add(IRow row, Stream targetStream)
      {
         if (_writer == null)
         {
            //If this is the first row, create the schema
            _fieldCount = row.Schema.Count;
            BuildWriter(row.Schema, targetStream);
            _lastKey = (IComparable)row.Get<object>(0);
            _keys = new ArrayList[_fieldCount];
            for (int i = 0; i < _fieldCount; i++)
            {
               _keys[i] = new ArrayList();
            }
            _keys[0].Add(_lastKey);
            for (int i = 1; i < _fieldCount; i++)
            {
               _keys[i].Add(row.Schema[i].DefaultValue);
            }
         }

         //If there is no dataset to fill yet, create it
         if (_ds == null)
         {
            CreateDataSet(null);
         }
         else
         {
            //Test to see if we need  to start a new RowGroup
            var newKey = (IComparable)row.Get<object>(0);
            if (_lastKey.CompareTo(newKey) != 0)
            {
               FlushDataSet();
               _lastKey = newKey;
               _keys[0].Add(_lastKey);
               for (int i = 1; i < _fieldCount; i++)
               {
                  _keys[i].Add(row.Schema[i].DefaultValue);
               }
               CreateDataSet(targetStream);
            }
         }

         //Copy the data into the dataset
         for (int i = 0; i < _fieldCount; i++)
         {
            var v = row.Get<object>(i);
            _ds[i].Add(v);
         }

      }

      private void CreateDataSet(Stream targetStream)
      {
         _ds = new ArrayList[_fieldCount];
         for (int i = 0; i < _fieldCount; i++)
         {
            _ds[i] = new ArrayList();
         }
         //if (targetStream != null)
         //{
         //   //Create the new row group by appending to the old one
         //   _writer = new ParquetWriter(_schema, targetStream, append: true);
         //}
      }

      private void FlushDataSet()
      {
         if (_ds == null) return;

         using (var rgWriter = _writer.CreateRowGroup(_ds[0].Count))
         {
            for (int i = 0; i < _fieldCount; i++)
            {
               var f = (DataField)_schema.Fields[i];
               rgWriter.WriteColumn(new DataColumn(f, _ds[i].ToArray()));
            }
         }
         _rowGroupCount++;
      }

      /// <summary>
      /// Appends a final rowset containing a single row with just the key column populated.
      /// This can be used to quickly identify which rowset is required for the given key
      /// </summary>
      private void FlushKeysRowSet()
      {
         if (_keys == null) return;

         using (var rgWriter = _writer.CreateRowGroup(_ds[0].Count))
         {
            for (int i = 0; i < _fieldCount; i++)
            {
               var f = (DataField)_schema.Fields[i];
               rgWriter.WriteColumn(new DataColumn(f, _keys[i].ToArray()));
            }
         }
         _rowGroupCount++;
      }

      private void BuildWriter(ISchema schema, Stream targetStream)
      {
         var fields = new List<Field>();
         foreach (IColumn column in schema)
         {
            var se = new DataField(column.Name, column.Type);
            fields.Add(se);
         }
         _schema = new Schema(fields);

         //Use better compression to keep the file size under 1GB
         _writer = new ParquetWriter(_schema, targetStream) { CompressionMethod = CompressionMethod.Gzip };
      }

      //private Row ToRow(IRow row)
      //{
      //   //you can get any type as object, it will be just unboxed

      //   var pqRow = new List<object>();
      //   for (int i = 0; i < _schema.Count; i++)
      //   {
      //      object value = row.Get<object>(i);
      //      pqRow.Add(value);
      //   }

      //   return new Row(pqRow);
      //}

      public void Dispose()
      {
         if (_writer == null) return;

         FlushDataSet();

         FlushKeysRowSet();

         _writer.Dispose();
         _writer = null;
      }
   }
}
