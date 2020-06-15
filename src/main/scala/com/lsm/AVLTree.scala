package com.lsm

import scala.collection.mutable

class AVLTree[A](implicit val ordering: Ordering[A]) extends mutable.SortedSet[A] {

  if (ordering eq null) throw new NullPointerException("ordering must not be null")

  private var _root: AVLNode = _
  private var _size = 0

  def root: AVLNode = _root

  override def size: Int = _size

  override def foreach[U](f: (A) => U): Unit = {
    val stack = mutable.Stack[AVLNode]()
    var current = root
    var done = false

    while (!done) {
      if (current != null) {
        stack.push(current)
        current = current.left
      } else if (stack.nonEmpty) {
        current = stack.pop()
        f.apply(current.key)

        current = current.right
      } else {
        done = true
      }
    }
  }

  override def isEmpty: Boolean = root == null

  def findNode(key: A): Option[AVLNode] = {
    var node = root
    while (node != null) {
      val cmp = ordering.compare(key, node.key)
      if (cmp == 0) return Some(node)
      node = node.matchNextChild(cmp)
    }
    None
  }

  def search(key: A): Option[A] = {
    var node = root
    while (node != null) {
      val cmp = ordering.compare(key, node.key)
      if (cmp == 0) return Some(node.key)
      node = node.matchNextChild(cmp)
    }
    None
  }

  override def min[B >: A](implicit cmp: Ordering[B]): A = minNode().key

  def minNode(): AVLNode = {
    if (root == null) throw new UnsupportedOperationException("empty tree")
    var node = root
    while (node.left != null) node = node.left
    node
  }

  override def max[B >: A](implicit cmp: Ordering[B]): A = maxNode().key

  def maxNode(): AVLNode = {
    if (root == null) throw new UnsupportedOperationException("empty tree")
    var node = root
    while (node.right != null) node = node.right
    node
  }

  override def addOne(key: A): AVLTree.this.type = {
    if (root == null) {
      _root = new AVLNode(key)
      _size += 1
      return this
    }

    var node = root
    var parent: AVLNode = null
    var cmp = 0

    while (node != null) {
      parent = node
      cmp = ordering.compare(key, node.key)
      if (cmp == 0) return this // duplicate
      node = node.matchNextChild(cmp)
    }

    val newNode = new AVLNode(key, parent)
    if (cmp <= 0) parent._left = newNode
    else parent._right = newNode

    while (parent != null) {
      cmp = ordering.compare(parent.key, key)
      if (cmp < 0) parent.balanceFactor -= 1
      else parent.balanceFactor += 1

      parent = parent.balanceFactor match {
        case -1 | 1 => parent.parent
        case x if x < -1 =>
          if (parent.right.balanceFactor == 1) rotateRight(parent.right)
          val newRoot = rotateLeft(parent)
          if (parent == root) _root = newRoot
          null
        case x if x > 1 =>
          if (parent.left.balanceFactor == -1) rotateLeft(parent.left)
          val newRoot = rotateRight(parent)
          if (parent == root) _root = newRoot
          null
        case _ => null
      }
    }

    _size += 1
    this
  }

  override def remove(key: A): Boolean = {
    var node = findNode(key).orNull
    if (node == null) return false

    if (node.left != null) {
      var max = node.left

      while (max.left != null || max.right != null) {
        while (max.right != null) max = max.right

        node._key = max.key
        if (max.left != null) {
          node = max
          max = max.left
        }
      }
      node._key = max.key
      node = max
    }

    if (node.right != null) {
      var min = node.right

      while (min.left != null || min.right != null) {
        while (min.left != null) min = min.left

        node._key = min.key
        if (min.right != null) {
          node = min
          min = min.right
        }
      }
      node._key = min.key
      node = min
    }

    var current = node
    var parent = node.parent
    while (parent != null) {
      parent.balanceFactor += (if (parent.left == current) -1 else 1)

      current = parent.balanceFactor match {
        case x if x < -1 =>
          if (parent.right.balanceFactor == 1) rotateRight(parent.right)
          val newRoot = rotateLeft(parent)
          if (parent == root) _root = newRoot
          newRoot
        case x if x > 1 =>
          if (parent.left.balanceFactor == -1) rotateLeft(parent.left)
          val newRoot = rotateRight(parent)
          if (parent == root) _root = newRoot
          newRoot
        case _ => parent
      }

      parent = current.balanceFactor match {
        case -1 | 1 => null
        case _ => current.parent
      }
    }

    if (node.parent != null) {
      if (node.parent.left == node) {
        node.parent._left = null
      } else {
        node.parent._right = null
      }
    }

    if (node == root) _root = null

    _size -= 1
    true
  }

  override def contains(elem: A): Boolean = findNode(elem).isDefined

  private def rotateLeft(node: AVLNode): AVLNode = {
    val rightNode = node.right
    node._right = rightNode.left
    if (node.right != null) node.right._parent = node

    rightNode._parent = node.parent
    if (rightNode.parent != null) {
      if (rightNode.parent.left == node) {
        rightNode.parent._left = rightNode
      } else {
        rightNode.parent._right = rightNode
      }
    }

    node._parent = rightNode
    rightNode._left = node

    node.balanceFactor += 1
    if (rightNode.balanceFactor < 0) {
      node.balanceFactor -= rightNode.balanceFactor
    }

    rightNode.balanceFactor += 1
    if (node.balanceFactor > 0) {
      rightNode.balanceFactor += node.balanceFactor
    }
    rightNode
  }

  private def rotateRight(node: AVLNode): AVLNode = {
    val leftNode = node.left
    node._left = leftNode.right
    if (node.left != null) node.left._parent = node

    leftNode._parent = node.parent
    if (leftNode.parent != null) {
      if (leftNode.parent.left == node) {
        leftNode.parent._left = leftNode
      } else {
        leftNode.parent._right = leftNode
      }
    }

    node._parent = leftNode
    leftNode._right = node

    node.balanceFactor -= 1
    if (leftNode.balanceFactor > 0) {
      node.balanceFactor -= leftNode.balanceFactor
    }

    leftNode.balanceFactor -= 1
    if (node.balanceFactor < 0) {
      leftNode.balanceFactor += node.balanceFactor
    }
    leftNode
  }

  class AVLNode private[AVLTree](k: A, p: AVLNode = null) {

    private[AVLTree] var _key: A = k

    def key: A = _key

    private[AVLTree] var _parent: AVLNode = p

    def parent: AVLNode = _parent

    private[AVLTree] var _left: AVLNode = _

    def left: AVLNode = _left

    private[AVLTree] var _right: AVLNode = _

    def right: AVLNode = _right

    private[AVLTree] var balanceFactor: Int = 0

    private[AVLTree] def selectNextChild(key: A): AVLNode = matchNextChild(ordering.compare(key, this.key))

    private[AVLTree] def matchNextChild(cmp: Int): AVLNode = cmp match {
      case x if x < 0 => left
      case x if x > 0 => right
      case _ => null
    }
  }

  override def clear(): Unit = foreach(remove)

  override def subtractOne(elem: A): AVLTree.this.type = {
    remove(elem)
    this
  }

  override def rangeImpl(from: Option[A], until: Option[A]): mutable.SortedSet[A] = ???

  override def iteratorFrom(start: A): Iterator[A] = ???

  override def iterator: Iterator[A] = {
    new Iterator[A]() {
      val _stack: mutable.Stack[AVLNode] = mutable.Stack.empty[AVLNode]
      var _current: AVLNode = root

      override def hasNext: Boolean = {
        _current != null || _stack.nonEmpty
      }

      override def next(): A = {
        while (_current !=  null) {
          _stack.push(_current)
          _current = _current.left
        }

        _current = _stack.pop()
        val res = _current.key
        _current = _current.right
        res
      }
    }
  }
}
