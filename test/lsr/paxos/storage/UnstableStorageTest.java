package lsr.paxos.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import lsr.paxos.storage.StableStorage;
import lsr.paxos.storage.UnstableStorage;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "unit" })
public class UnstableStorageTest {
	private StableStorage _storage;

	@BeforeMethod
	public void setUp() {
		_storage = new UnstableStorage();
	}

	public void testInitialView() {
		assertEquals(0, _storage.getView());
	}

	public void testSetHigherView() {
		_storage.setView(5);
		assertEquals(5, _storage.getView());

		_storage.setView(9);
		assertEquals(9, _storage.getView());
	}

	public void testSetLowerView() {
		_storage.setView(5);
		assertEquals(5, _storage.getView());

		try {
			_storage.setView(3);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail();
	}

	public void testSetEqualView() {
		_storage.setView(5);
		assertEquals(5, _storage.getView());
		try {
			_storage.setView(5);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail();
	}
}
