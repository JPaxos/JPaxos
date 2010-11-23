package lsr.paxos.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;

public class UnstableStorageTest {
	private StableStorage storage;

	@Before
	public void setUp() {
		storage = new UnstableStorage();
	}

	@Test
	public void testInitialView() {
		assertEquals(0, storage.getView());
	}

	@Test
	public void testSetHigherView() {
		storage.setView(5);
		assertEquals(5, storage.getView());

		storage.setView(9);
		assertEquals(9, storage.getView());
	}

	@Test
	public void testSetLowerView() {
		storage.setView(5);
		assertEquals(5, storage.getView());

		try {
			storage.setView(3);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail();
	}

	@Test
	public void testSetEqualView() {
		storage.setView(5);
		assertEquals(5, storage.getView());
		try {
			storage.setView(5);
		} catch (IllegalArgumentException e) {
			return;
		}
		fail();
	}
}
