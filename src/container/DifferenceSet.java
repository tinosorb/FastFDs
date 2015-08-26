package container;

import container.AgreeSet;

import org.apache.lucene.util.OpenBitSet;

import algorithm.BitSetUtil;

public class DifferenceSet extends AgreeSet {

    public DifferenceSet(OpenBitSet obs) {

        this.attributes = obs;
    }

    public DifferenceSet() {

        this(new OpenBitSet());
    }

    @Override
    public String toString_() {

        return "diff(" + BitSetUtil.convertToIntList(this.attributes).toString()
                + ")";
    }
}
