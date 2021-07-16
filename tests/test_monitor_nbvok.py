from datetime import date

import pytest

from seiso.console.monitor_nbvok import find_changes
from seiso.services.nb import Vocabulary, Concept


def test_find_changes():
    prev = Vocabulary(marc_code='test', concepts={
        '#URI1': Concept(vocabulary='test', uri='#URI1',
                      label_nb='Ubåt', label_nn='Kavbåt', altlabel_nb={'Undervannsbåt'}),
    })
    curr = Vocabulary(marc_code='test', concepts={
        '#URI1': Concept(vocabulary='test', uri='#URI1',
                      label_nb='Undervannsbåt', label_nn='Undervassbåt', altlabel_nb={'Ubåt'}),
    })
    changes = list(find_changes(prev, curr))

    assert len(changes) == 3
    assert set(changes) == {
        "| <#URI1> | Foretrukket term (bokmål) endret fra 'Ubåt' til 'Undervannsbåt' |",
        "| <#URI1> | Foretrukket term (nynorsk) endret fra 'Kavbåt' til 'Undervassbåt' |",
        "| <#URI1> | Henvisningstermer (bokmål) endret fra {'Undervannsbåt'} til {'Ubåt'} |",
    }
