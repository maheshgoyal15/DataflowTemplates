/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigtable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigtable.BigtableChangeStreamsToBigtable.ChangeStreamMutationToBigtableMutationFn;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigtableChangeStreamsToBigtable}. */
@RunWith(JUnit4.class)
public final class BigtableChangeStreamsToBigtableTest {

  @Test
  public void testEntriesToMutations() {
    // Mock SetCell
    SetCell setCell = mock(SetCell.class);
    when(setCell.getFamilyName()).thenReturn("cf1");
    when(setCell.getQualifier()).thenReturn(ByteString.copyFromUtf8("col1"));
    when(setCell.getTimestamp()).thenReturn(1000L);
    when(setCell.getValue()).thenReturn(ByteString.copyFromUtf8("value1"));

    // Mock DeleteCells
    DeleteCells deleteCells = mock(DeleteCells.class);
    when(deleteCells.getFamilyName()).thenReturn("cf1");
    when(deleteCells.getQualifier()).thenReturn(ByteString.copyFromUtf8("col2"));
    TimestampRange timestampRange = TimestampRange.create(1000L, 2000L);
    when(deleteCells.getTimestampRange()).thenReturn(timestampRange);

    // Mock DeleteFamily
    DeleteFamily deleteFamily = mock(DeleteFamily.class);
    when(deleteFamily.getFamilyName()).thenReturn("cf2");

    List<Entry> entries = ImmutableList.of(setCell, deleteCells, deleteFamily);

    List<Mutation> mutations = ChangeStreamMutationToBigtableMutationFn.entriesToMutations(entries);

    assertEquals(3, mutations.size());

    // Verify SetCell
    Mutation m1 = mutations.get(0);
    assertTrue(m1.hasSetCell());
    assertEquals("cf1", m1.getSetCell().getFamilyName());
    assertEquals("col1", m1.getSetCell().getColumnQualifier().toStringUtf8());
    assertEquals(1000L, m1.getSetCell().getTimestampMicros());
    assertEquals("value1", m1.getSetCell().getValue().toStringUtf8());

    // Verify DeleteCells
    Mutation m2 = mutations.get(1);
    assertTrue(m2.hasDeleteFromColumn());
    assertEquals("cf1", m2.getDeleteFromColumn().getFamilyName());
    assertEquals("col2", m2.getDeleteFromColumn().getColumnQualifier().toStringUtf8());
    assertEquals(1000L, m2.getDeleteFromColumn().getTimeRange().getStartTimestampMicros());
    assertEquals(2000L, m2.getDeleteFromColumn().getTimeRange().getEndTimestampMicros());

    // Verify DeleteFamily
    Mutation m3 = mutations.get(2);
    assertTrue(m3.hasDeleteFromFamily());
    assertEquals("cf2", m3.getDeleteFromFamily().getFamilyName());
  }
}
