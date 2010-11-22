package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

public class UnstableStorageTest {
	private StableStorage _storage;

	@Before
	public void setUp() {
		_storage = new UnstableStorage();
	}

	@Test
	public void testInitialView() {
		assertEquals(0, _storage.getView());
	}

	@Test
	public void testSetHigherView() {
		_storage.setView(5);
		assertEquals(5, _storage.getView());

		_storage.setView(9);
		assertEquals(9, _storage.getView());
	}

	@Test
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

	@Test
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
