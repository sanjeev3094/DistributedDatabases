package com.adb.general;

import gudusoft.gsqlparser.nodes.IExpressionVisitor;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TParseTreeNode;

import java.util.ArrayList;

import com.adb.fragment.DataLocalisation;

public class ModifyWhere implements IExpressionVisitor
{

	private TExpression condition;
	private int flag;
	private ArrayList<Condition> ConditionValArray;
	private ArrayList<Condition> ConditionAttrArray;

	public ModifyWhere(TExpression expr,int flag)
	{
		this.condition = expr;
		this.flag=flag;
	}
	public ModifyWhere(TExpression expr,int flag,ArrayList<Condition> arr,ArrayList<Condition> arr1)
	{
		this.condition = expr;
		this.flag=flag;
		this.ConditionValArray=arr;
		this.ConditionAttrArray=arr1;
	}

	public void printColumn()
	{
		this.condition.inOrderTraverse(this);
	}
	boolean is_compare_condition(int t)
	{
		return ((t == TExpression.simple_comparison_conditions)
				|| (t == TExpression.group_comparison_conditions)
				|| (t == TExpression.in_conditions) || (t == TExpression.pattern_matching_conditions));// ||(t==TExpression.logical_conditions_and));

	}

		public boolean exprVisit(TParseTreeNode pnode, boolean pIsLeafNode)
		{
			TExpression lcexpr = (TExpression) pnode;
			if (!lcexpr.isLeaf())
			{
				// System.out.println("###"+lcexpr.toString());
			}
			if (is_compare_condition(lcexpr.getExpressionType()))
			{
				// System.out.println("Expr : "+lcexpr.toString());
				TExpression leftExpr = (TExpression) lcexpr.getLeftOperand();
				TExpression rightExpr = (TExpression) lcexpr.getRightOperand();
				if(flag==0)
				{
				if (leftExpr.isLeaf())
				{
					if (rightExpr.isLeaf())
					{
						String rightstr=rightExpr.toString();
						if ((rightstr.charAt(0) == '\'' && rightstr.charAt(rightstr.length() - 1) == '\'')
								|| (rightstr.charAt(0) == '\"' && rightstr
										.charAt(rightstr.length() - 1) == '\"') ||rightstr.matches("\\d+"))
						{
							Condition cond=new Condition(leftExpr.toString(),lcexpr.getComparisonOperator().toString(),rightExpr.toString());
							ConditionValArray.add(cond);
						}
						else 
						{
							Condition cond=new Condition(leftExpr.toString(),lcexpr.getComparisonOperator().toString(),rightExpr.toString());
							ConditionAttrArray.add(cond);
						}
					}
					
				}
				}
				else
				{

				if (leftExpr.isLeaf())
				{
					Attribute leftattr = AttributeCheck.check(leftExpr.toString());
					String lefttype = leftattr.getType();
					// System.out.println("###Type "+lefttype);
					if (rightExpr.isLeaf())
					{
						String rightstr = rightExpr.toString();
						//System.out.println(rightstr + ";" + lefttype);
						if (rightstr.matches("\\d+"))
						{
							if (lefttype.startsWith("int"))
							{
								//System.out.println("Integer");
							}
							else
							{
								System.out.println("Error in where condition");
								System.exit(-1);
							}
						}
						else
							if (lefttype.contains("varchar") || lefttype.contains("date"))
							{
								// System.out.println("String");
								if ((rightstr.charAt(0) == '\'' && rightstr
										.charAt(rightstr.length() - 1) == '\'')
										|| (rightstr.charAt(0) == '\"' && rightstr
												.charAt(rightstr.length() - 1) == '\"'))
								{

								}
								else
								{
									// System.out.println("Here");
									Attribute rightattr = AttributeCheck.check(rightExpr.toString());
									String righttype = rightattr.getType();
									if ((lefttype.contains("varchar") && righttype.contains("varchar"))|| lefttype.equals(righttype))
									{
										String attrstr = rightattr.getTable().getName() + "."+ rightattr.getName();
										String modattrstr;
										if (flag==2)
										{
											DataLocalisation dl=new DataLocalisation();
											modattrstr=dl.getNewAttribute(attrstr);
											attrstr=modattrstr;
										}
										rightExpr.setString(attrstr);
									}
									else
									{
										System.out.println("Error in where condition");
										System.exit(-1);
									}

								}
							}
							else
							{
								System.out.println("Error in where condition");
								System.exit(-1);
							}

					}
					String attrstr = leftattr.getTable().getName() + "."+ leftattr.getName();
					String modattrstr;
					if(flag==2)
					{
						DataLocalisation dl=new DataLocalisation();
						modattrstr=dl.getNewAttribute(attrstr);
						leftExpr.setString(modattrstr);
						attrstr=modattrstr;
					}
					else
						leftExpr.setString(attrstr);
				}
				}
				/*
				 * System.out.println( "column: " + leftExpr.toString( ) );
				 * System.out.println(leftExpr.getRightOperand()); if (
				 * lcexpr.getComparisonOperator( ) != null ) { System.out.println(
				 * "Operator: " + lcexpr.getComparisonOperator( ).astext ); } else {
				 * System
				 * .out.println("Operatorss="+lcexpr.getOperatorToken().astext); }
				 * System.out.println(lcexpr.isLeaf()); System.out.println(
				 * "value: " + rightExpr.toString( ) ); System.out.println( "" );
				 */
			}
			return true;
		}}

